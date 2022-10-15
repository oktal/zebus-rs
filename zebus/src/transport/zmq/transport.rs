use prost::Message;
use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, Read, Write},
    sync::{self, mpsc},
    thread::JoinHandle,
};

use crate::{
    transport::{
        zmq::{ZmqSocketOptions, ZmqTransportConfiguration},
        MessageReceived, SendContext, Transport, TransportMessage,
    },
    Peer, PeerId,
};

use super::{inbound, outbound::ZmqOutboundSocket};
use super::{inbound::ZmqInboundSocket, outbound};

const OUTBOUND_THREAD_NAME: &'static str = "outbound";
const INBOUND_THREAD_NAME: &'static str = "inbound";

/// Associated Error type with zmq
pub enum Error {
    /// Inbound error
    Inbound(inbound::Error),

    /// Outbound error
    Outbound(outbound::Error),

    /// IO Error
    Io(io::Error),

    /// Protobuf message encoding error
    Encode(prost::EncodeError),

    /// Protobuf message decoding error
    Decode(prost::DecodeError),

    /// An operation was attempted while the [`ZmqTransport`] was in an invalid state for the
    /// operation
    InvalidOperation,
}

enum OuboundSocketAction {
    Send {
        message: TransportMessage,
        peers: Vec<Peer>,
        context: SendContext,
    },

    Disconnect {
        peer_id: PeerId,
    },
}

enum Inner {
    Unconfigured {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
    },
    Configured {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
        peer_id: PeerId,
        environment: String,
        on_message_received: Box<dyn MessageReceived>,
    },
    Started {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
        peer_id: PeerId,
        environment: String,
        on_message_received: Box<dyn MessageReceived>,

        context: zmq::Context,

        inbound_endpoint: String,
        inbound_thread: JoinHandle<()>,

        outbound_thread: JoinHandle<()>,
        actions_tx: sync::mpsc::Sender<OuboundSocketAction>,

        // TODO(oktal): Replace with a oneshot channel ?
        shutdown_tx: mpsc::Sender<()>,
    },
}

/// Zmq-based [`Transport`] implementation
pub struct ZmqTransport {
    // Invariant: the underlying [`Option`] is always `Some`.
    // We use the `Option` as a way to move the inner state from a mutable
    // reference when transitioning between states
    inner: Option<Inner>,
}

impl ZmqTransport {
    pub fn new(configuration: ZmqTransportConfiguration, options: ZmqSocketOptions) -> Self {
        Self {
            inner: Some(Inner::Unconfigured {
                configuration,
                options,
            }),
        }
    }
}

struct InboundWorker<C: Fn(&[u8]) + 'static + Send> {
    shutdown_rx: mpsc::Receiver<()>,
    inbound_socket: ZmqInboundSocket,
    on_message_received: C,
}

impl<C: Fn(&[u8]) + 'static + Send> InboundWorker<C> {
    fn start(
        inbound_socket: ZmqInboundSocket,
        on_message_received: C,
    ) -> Result<(mpsc::Sender<()>, JoinHandle<()>), Error> {
        // Create channel to shutdown inbound worker thread
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        // Create inbound worker
        let mut inbound_worker = InboundWorker {
            shutdown_rx,
            inbound_socket,
            on_message_received,
        };

        // Spawn inbound thread
        let inbound_thread = std::thread::Builder::new()
            .name(INBOUND_THREAD_NAME.into())
            .spawn(move || {
                inbound_worker.run();
            })
            .map_err(Error::Io)?;

        Ok((shutdown_tx, inbound_thread))
    }

    fn run(&mut self) {
        let mut rcv_buf = vec![0u8; 1024];
        loop {
            if let Err(_) = self.shutdown_rx.try_recv() {
                break;
            }

            // TODO(oktal): Handle error properly
            self.inbound_socket.read_to_end(&mut rcv_buf).unwrap();

            rcv_buf.clear();
        }
    }
}

struct OutboundWorker {
    actions_rx: mpsc::Receiver<OuboundSocketAction>,
    context: zmq::Context,
    outbound_sockets: HashMap<PeerId, ZmqOutboundSocket>,
}

impl OutboundWorker {
    fn start(
        context: zmq::Context,
    ) -> Result<(mpsc::Sender<OuboundSocketAction>, JoinHandle<()>), Error> {
        // Create outbound channel for outbound socket operations
        let (actions_tx, actions_rx) = mpsc::channel();

        // Create outbound worker
        let mut worker = OutboundWorker {
            context,
            actions_rx,
            outbound_sockets: HashMap::new(),
        };

        // Spawn outbound thread
        let outbound_thread = std::thread::Builder::new()
            .name(OUTBOUND_THREAD_NAME.into())
            .spawn(move || {
                // TODO(oktal): Bubble up the error to the JoinHandle
                worker.run();
            })
            .map_err(Error::Io)?;

        Ok((actions_tx, outbound_thread))
    }

    fn run(&mut self) -> Result<(), Error> {
        let mut encode_buf = vec![0u8; 1024];

        while let Ok(action) = self.actions_rx.recv() {
            self.handle_action(action, &mut encode_buf)?;
        }

        Ok(())
    }

    fn handle_action(
        &mut self,
        action: OuboundSocketAction,
        encode_buf: &mut Vec<u8>,
    ) -> Result<(), Error> {
        match action {
            OuboundSocketAction::Send {
                message,
                peers,
                context,
            } => self.handle_send(message, peers, context, encode_buf),
            OuboundSocketAction::Disconnect { peer_id } => unimplemented!(),
        }
    }

    fn handle_send(
        &mut self,
        mut message: TransportMessage,
        peers: Vec<Peer>,
        context: SendContext,
        encode_buf: &mut Vec<u8>,
    ) -> Result<(), Error> {
        for peer in peers {
            let was_persisted = context.was_persisted(&peer.id);
            message.was_persisted = Some(was_persisted);
            self.send_to(&peer, &message, encode_buf)?;
        }

        // TODO(oktal): handle persistent peer

        Ok(())
    }

    fn send_to(
        &mut self,
        peer: &Peer,
        message: &TransportMessage,
        encode_buf: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let socket = self.get_socket(peer)?;
        let bytes = Self::encode(message, encode_buf)?;

        socket.write_all(bytes).map_err(Error::Io)
    }

    fn get_socket(&mut self, peer: &Peer) -> Result<&mut ZmqOutboundSocket, Error> {
        let socket = self
            .outbound_sockets
            .entry(peer.id.clone())
            .or_insert_with_key(|peer_id| {
                ZmqOutboundSocket::new(self.context.clone(), peer_id.clone())
            });
        // TODO(oktal): Handle reconnection to a different endpoint
        if !socket.is_connected() {
            socket.connect(&peer.endpoint).map_err(Error::Outbound)?;
        }
        Ok(socket)
    }

    fn encode<'a>(
        message: &TransportMessage,
        encode_buf: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], Error> {
        encode_buf.clear();
        message.encode(encode_buf).map_err(Error::Encode)?;

        Ok(&encode_buf[..])
    }
}

impl Transport for ZmqTransport {
    type Err = Error;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        on_message_received: impl MessageReceived,
    ) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Unconfigured {
                configuration,
                options,
            }) => (
                // Transition to Configured state
                Some(Inner::Configured {
                    configuration,
                    options,
                    peer_id,
                    environment,
                    on_message_received: Box::new(on_message_received),
                }),
                Ok(()),
            ),
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn start(&mut self) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                options,
                peer_id,
                environment,
                on_message_received,
            }) => {
                // Create zmq context
                let context = zmq::Context::new();

                // Create the inbound socket
                let mut inbound_socket = ZmqInboundSocket::new(
                    context.clone(),
                    peer_id.clone(),
                    configuration.inbound_endpoint.clone(),
                    options,
                );

                // Bind the inbound socket
                let inbound_endpoint = inbound_socket.bind().map_err(Error::Inbound)?;

                // Start outbound worker
                let (actions_tx, outbound_thread) = OutboundWorker::start(context.clone())?;

                // Start inbound worker
                let (shutdown_tx, inbound_thread) =
                    InboundWorker::start(inbound_socket, |bytes| {})?;

                (
                    // Transition to Started state
                    Some(Inner::Started {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        on_message_received,
                        context,
                        inbound_endpoint,
                        inbound_thread,
                        outbound_thread,
                        actions_tx,
                        shutdown_tx,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<(), Self::Err> {
        todo!()
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref inbound_endpoint,
                ..
            }) => Ok(Cow::Borrowed(inbound_endpoint.as_str())),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<(), Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { ref actions_tx, .. }) => {
                let peers = peers.collect();

                // TODO(oktal): Can `send` fail ?
                actions_tx
                    .send(OuboundSocketAction::Send {
                        message,
                        peers,
                        context,
                    })
                    .expect("unexpected send failure");

                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }
}
