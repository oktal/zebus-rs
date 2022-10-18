use prost::Message;
use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, Read, Write},
    sync::{self, mpsc},
    thread::JoinHandle,
};
use thiserror::Error;

use crate::{
    transport::{
        zmq::{ZmqSocketOptions, ZmqTransportConfiguration},
        Receiver, SendContext, Transport, TransportMessage,
    },
    Peer, PeerId,
};

use super::{inbound, outbound::ZmqOutboundSocket};
use super::{inbound::ZmqInboundSocket, outbound};

const OUTBOUND_THREAD_NAME: &'static str = "outbound";
const INBOUND_THREAD_NAME: &'static str = "inbound";

/// Associated Error type with zmq
#[derive(Debug, Error)]
pub enum Error {
    /// Inbound error
    #[error("receive error {0}")]
    Inbound(inbound::Error),

    /// Outbound error
    #[error("send error {0}")]
    Outbound(outbound::Error),

    /// A function call that returns an [`std::ffi::OsStr`] or [`std::ffi::OsString`] yield
    /// invalid UTF-8 sequence
    #[error("an invalid UTF-8 sequence was returned by an ffi function call")]
    InvalidUtf8,

    /// IO Error
    #[error("IO {0}")]
    Io(io::Error),

    /// Protobuf message encoding error
    #[error("error encoding protobuf message {0}")]
    Encode(prost::EncodeError),

    /// Protobuf message decoding error
    #[error("error decoding protobuf message {0}")]
    Decode(prost::DecodeError),

    /// An operation was attempted while the [`ZmqTransport`] was in an invalid state for the
    /// operation
    #[error("An operation was attempted while the transport was not in a valid state")]
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
    },
    Started {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
        peer_id: PeerId,
        environment: String,

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

struct InboundWorker {
    shutdown_rx: mpsc::Receiver<()>,
    inbound_socket: ZmqInboundSocket,
    rcv_tx: tokio::sync::mpsc::Sender<TransportMessage>,
}

impl InboundWorker {
    fn start(
        inbound_socket: ZmqInboundSocket,
    ) -> Result<(mpsc::Sender<()>, Receiver, JoinHandle<()>), Error> {
        // Create channel to shutdown inbound worker thread
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        // Create the channel to receive transport messages
        // TODO(oktal): remove hardcoded bound limit
        let (rcv_tx, rcv_rx) = tokio::sync::mpsc::channel(1024);

        // Create inbound worker
        let mut inbound_worker = InboundWorker {
            shutdown_rx,
            inbound_socket,
            rcv_tx,
        };

        // Spawn inbound thread
        let inbound_thread = std::thread::Builder::new()
            .name(INBOUND_THREAD_NAME.into())
            .spawn(move || {
                inbound_worker.run();
            })
            .map_err(Error::Io)?;

        Ok((shutdown_tx, rcv_rx, inbound_thread))
    }

    fn run(&mut self) {
        loop {
            if let Ok(_) = self.shutdown_rx.try_recv() {
                break;
            }
            let mut rcv_buf = [0u8; 4096];

            // TODO(oktal): Handle error properly
            match self.inbound_socket.read(&mut rcv_buf[..]) {
                Ok(size) => {
                    match TransportMessage::decode(&rcv_buf[..size]) {
                        Ok(message) => self.rcv_tx.blocking_send(message).unwrap(),
                        Err(e) => eprintln!("Failed to decode: {e}. bytes {rcv_buf:?}"),
                    };
                }
                Err(_) => {
                    //println!("{}", e.kind());
                }
            };
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

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err> {
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
                }),
                Ok(()),
            ),
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn start(&mut self) -> Result<Receiver, Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                options,
                peer_id,
                environment,
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
                let mut inbound_endpoint = inbound_socket.bind().map_err(Error::Inbound)?;
                let hostname = gethostname::gethostname();
                let host_str = hostname.to_str().ok_or(Error::InvalidUtf8)?;
                inbound_endpoint = inbound_endpoint.replace("0.0.0.0", &host_str);

                // Start outbound worker
                let (actions_tx, outbound_thread) = OutboundWorker::start(context.clone())?;

                // Start inbound worker
                let (shutdown_tx, rcv_rx, inbound_thread) = InboundWorker::start(inbound_socket)?;

                (
                    // Transition to Started state
                    Some(Inner::Started {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        context,
                        inbound_endpoint,
                        inbound_thread,
                        outbound_thread,
                        actions_tx,
                        shutdown_tx,
                    }),
                    Ok(rcv_rx),
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
