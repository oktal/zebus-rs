use prost::Message;
use std::{
    borrow::Cow,
    collections::HashMap,
    io::{self, Write},
    thread::JoinHandle,
};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use crate::{
    directory::{self, event::PeerEvent},
    transport::{
        zmq::{ZmqSocketOptions, ZmqTransportConfiguration},
        SendContext, Transport, TransportMessage,
    },
    Peer, PeerId,
};

#[cfg(windows)]
use std::io::Read;

#[cfg(unix)]
use tokio::io::AsyncReadExt;

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

#[derive(Debug)]
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
        directory_rx: directory::EventStream,
    },
    Started {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
        peer_id: PeerId,
        environment: String,

        context: zmq::Context,

        inbound_endpoint: String,
        inbound_handle: JoinHandle<()>,

        outbound_handle: JoinHandle<Result<OutboundHandle, Error>>,
        actions_tx: mpsc::Sender<OuboundSocketAction>,
        rcv_tx: broadcast::Sender<TransportMessage>,

        shutdown_tx: broadcast::Sender<()>,
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
    inbound_socket: ZmqInboundSocket,
    rcv_tx: broadcast::Sender<TransportMessage>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl InboundWorker {
    fn start(
        inbound_socket: ZmqInboundSocket,
        rcv_tx: broadcast::Sender<TransportMessage>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<JoinHandle<()>, Error> {
        // Subscribe to shutdown channel
        let shutdown_rx = shutdown_tx.subscribe();

        // Create inbound worker
        let worker = InboundWorker {
            inbound_socket,
            rcv_tx,
            shutdown_rx,
        };

        // Create tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::Io)?;

        // Spawn inbound thread
        let inbound_thread = std::thread::Builder::new()
            .name(INBOUND_THREAD_NAME.into())
            .spawn(move || rt.block_on(worker.run()))
            .map_err(Error::Io)?;

        Ok(inbound_thread)
    }

    #[cfg(unix)]
    async fn run(mut self) {
        self.inbound_socket.enable_polling().unwrap();

        let mut rcv_buf = [0u8; 4096];

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => { break },
                // TODO(oktal): Handle error properly
                Ok(size) = self.inbound_socket.read(&mut rcv_buf[..]) => {
                    match TransportMessage::decode(&rcv_buf[..size]) {
                        Ok(message) =>  {
                            let _ = self.rcv_tx.send(message);
                        },
                        Err(e) => eprintln!("Failed to decode: {e}. bytes {rcv_buf:?}"),
                    };
                }
            }
        }

        let _ = self.close();
    }

    #[cfg(windows)]
    async fn run(mut self) {
        loop {
            if self.shutdown_rx.try_recv().is_ok() {
                break;
            }

            let mut rcv_buf = [0u8; 4096];

            // TODO(oktal): Handle error properly
            if let Ok(size) = self.inbound_socket.read(&mut rcv_buf[..]) {
                match TransportMessage::decode(&rcv_buf[..size]) {
                    Ok(message) => {
                        let _ = self.rcv_tx.send(message);
                    }
                    Err(e) => eprintln!("Failed to decode: {e}. bytes {rcv_buf:?}"),
                };
            };
        }

        let _ = self.close();
    }

    fn close(&mut self) -> Result<(), Error> {
        if let Err(e) = self.inbound_socket.close() {
            warn!("failed to unbind: {e}");
        }

        Ok(())
    }
}

struct OutboundHandle {
    directory_rx: directory::EventStream,
}

struct OutboundWorker {
    context: zmq::Context,
    peer_id: PeerId,
    outbound_sockets: HashMap<PeerId, ZmqOutboundSocket>,
    actions_rx: mpsc::Receiver<OuboundSocketAction>,
    directory_rx: directory::EventStream,
    shutdown_rx: broadcast::Receiver<()>,
}

impl OutboundWorker {
    fn start(
        context: zmq::Context,
        peer_id: PeerId,
        directory_rx: directory::EventStream,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<
        (
            mpsc::Sender<OuboundSocketAction>,
            JoinHandle<Result<OutboundHandle, Error>>,
        ),
        Error,
    > {
        // Subscribe to shutdown channel
        let shutdown_rx = shutdown_tx.subscribe();

        // Create outbound channel for outbound socket operations
        // TODO(oktal): remove hardcoded bound limit
        let (actions_tx, actions_rx) = mpsc::channel(128);

        // Create outbound worker
        let worker = OutboundWorker {
            context,
            peer_id,
            outbound_sockets: HashMap::new(),
            actions_rx,
            directory_rx,
            shutdown_rx,
        };

        // Create tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::Io)?;

        // Spawn outbound thread
        let outbound_thread = std::thread::Builder::new()
            .name(OUTBOUND_THREAD_NAME.into())
            .spawn(move || rt.block_on(worker.run()))
            .map_err(Error::Io)?;

        Ok((actions_tx, outbound_thread))
    }

    async fn run(mut self) -> Result<OutboundHandle, Error> {
        let mut encode_buf = vec![0u8; 1024];

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => { break; },
                Some(action) = self.actions_rx.recv() => {
                    self.handle_action(action, &mut encode_buf)?;
                },
                Some(event) = self.directory_rx.next()=> {
                    self.handle_event(event)?;
                },
            }
        }

        self.disconnect_all()?;

        Ok(OutboundHandle {
            directory_rx: self.directory_rx,
        })
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
            OuboundSocketAction::Disconnect { peer_id } => self.disconnect(&peer_id),
        }
    }

    fn handle_event(&mut self, event: PeerEvent) -> Result<(), Error> {
        if event.peer_id() == &self.peer_id {
            return Ok(());
        }

        match event {
            PeerEvent::Decomissionned(peer_id) if !peer_id.is_persistence() => {
                self.disconnect(&peer_id)
            }
            // If a previously existing peer starts up with a new endpoint, make sure to disconnect
            // the previous socket to avoid keeping stale sockets
            PeerEvent::Started(peer_id) => self.disconnect(&peer_id),
            _ => Ok(()),
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
        let socket = self.get_connected_socket(&peer)?;
        let bytes = Self::encode(message, encode_buf)?;

        socket.write_all(bytes).map_err(Error::Io)
    }

    fn get_socket(&mut self, peer_id: &PeerId) -> Result<&mut ZmqOutboundSocket, Error> {
        let socket = self
            .outbound_sockets
            .entry(peer_id.clone())
            .or_insert_with_key(|peer_id| {
                ZmqOutboundSocket::new(self.context.clone(), peer_id.clone())
            });
        Ok(socket)
    }

    fn get_connected_socket(&mut self, peer: &Peer) -> Result<&mut ZmqOutboundSocket, Error> {
        let socket = self.get_socket(&peer.id)?;
        // TODO(oktal): Handle reconnection to a different endpoint
        if !socket.is_connected() {
            socket.connect(&peer.endpoint).map_err(Error::Outbound)?;
            info!("connected to {peer}");
        }
        Ok(socket)
    }

    fn disconnect(&mut self, peer_id: &PeerId) -> Result<(), Error> {
        if let Some(mut socket) = self.outbound_sockets.remove(peer_id) {
            socket.disconnect().map_err(Error::Outbound)?;
            // Dropping the socket will close the underlying zmq file descriptor
            drop(socket);
        }

        Ok(())
    }

    fn disconnect_all(&mut self) -> Result<(), Error> {
        for (peer_id, mut socket) in self.outbound_sockets.drain() {
            let endpoint = socket.endpoint().unwrap_or("NA").to_string();

            debug!("disconnecting outbound socket to peer {peer_id} [{endpoint}] ...");

            if let Err(e) = socket.disconnect() {
                warn!("failed to disconnect socket to peer {peer_id} [{endpoint}]: {e}");
            }

            debug!("... socket to {peer_id} [{endpoint}] disconnected");
        }

        Ok(())
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
    type MessageStream = crate::sync::stream::BroadcastStream<TransportMessage>;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory_rx: directory::EventStream,
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
                    directory_rx,
                }),
                Ok(()),
            ),
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
        match self.inner {
            Some(Inner::Started { ref rcv_tx, .. }) => Ok(rcv_tx.subscribe().into()),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn start(&mut self) -> Result<(), Self::Err> {
        info!("starting zmq transport...");

        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                options,
                peer_id,
                environment,
                directory_rx,
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
                info!("socket bound to endpoint {inbound_endpoint}");

                // Create shutdown broadcast signal
                let (shutdown_tx, _shutdown_rx) = broadcast::channel(16);

                // Start outbound worker
                let (actions_tx, oubound_worker) = OutboundWorker::start(
                    context.clone(),
                    peer_id.clone(),
                    directory_rx,
                    shutdown_tx.clone(),
                )?;

                // Create broadcast channel for transport message reception
                let (rcv_tx, _rcv_rx) = broadcast::channel(128);

                // Start inbound worker
                let inbound_handle =
                    InboundWorker::start(inbound_socket, rcv_tx.clone(), shutdown_tx.clone())?;

                (
                    // Transition to Started state
                    Some(Inner::Started {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        context,
                        inbound_endpoint,
                        inbound_handle,
                        outbound_handle: oubound_worker,
                        actions_tx,
                        rcv_tx,
                        shutdown_tx,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        info!("... started");
        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<(), Self::Err> {
        info!("stopping zmq transport...");

        let (inner, res) = match self.inner.take() {
            Some(Inner::Started {
                configuration,
                options,
                inbound_handle,
                outbound_handle,
                shutdown_tx,
                ..
            }) => {
                // Shutdown inbound and outbound worker
                let _ = shutdown_tx.send(());

                // Wait for the inbound worker to stop;
                if let Err(_) = inbound_handle.join() {
                    error!("inbound worker panic'ed");
                }

                // Wait for the inbound worker to stop;
                if let Err(_) = outbound_handle.join() {
                    error!("outbound worker panic'ed");
                }

                // Transition to Configured state
                (
                    Some(Inner::Unconfigured {
                        configuration,
                        options,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        info!("... stopped");
        self.inner = inner;
        res
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn environment(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured {
                ref environment, ..
            })
            | Some(Inner::Started {
                ref environment, ..
            }) => Ok(Cow::Borrowed(environment.as_str())),
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

                actions_tx
                    .try_send(OuboundSocketAction::Send {
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
