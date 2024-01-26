use futures_util::FutureExt;
use prost::Message;
use std::{borrow::Cow, collections::HashMap, io::Write, thread::JoinHandle};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    bus::{BusEvent, BusEventStream},
    directory::event::PeerEvent,
    sync::stream::{BoxEventStream, EventStream},
    transport::{
        self,
        future::SendFuture,
        zmq::{ZmqSocketOptions, ZmqTransportConfiguration},
        SendContext, Transport, TransportMessage,
    },
    Peer, PeerId,
};

#[cfg(windows)]
use std::io::Read;

#[cfg(unix)]
use tokio::io::AsyncReadExt;

use super::close::{self, EndOfStream};
use super::outbound::ZmqOutboundSocket;
use super::{inbound::ZmqInboundSocket, ZmqError};

const OUTBOUND_THREAD_NAME: &'static str = "outbound";
const INBOUND_THREAD_NAME: &'static str = "inbound";

#[derive(Debug)]
pub enum OutboundAction {
    /// Send a message to a list of peers
    Send {
        message: TransportMessage,
        peers: Vec<Peer>,
        context: SendContext,
    },

    /// End stream of a peer
    EndStream { peer_id: PeerId },
}

impl From<transport::future::SendEntry> for OutboundAction {
    fn from(value: transport::future::SendEntry) -> Self {
        Self::Send {
            peers: value.peers,
            message: value.message,
            context: SendContext::default(),
        }
    }
}

impl OutboundAction {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Send { .. } => "SendMessage",
            Self::EndStream { .. } => "EndStream",
        }
    }
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
        event_rx: BoxEventStream<BusEvent>,
    },
    Started {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
        peer_id: PeerId,
        environment: String,

        inbound_endpoint: String,
        inbound_handle: JoinHandle<()>,

        outbound_handle: JoinHandle<Result<OutboundHandle, ZmqError>>,
        actions_tx: mpsc::Sender<OutboundAction>,
        rcv_tx: broadcast::Sender<TransportMessage>,

        shutdown_tx: CancellationToken,
        shutdown_rx: CancellationToken,
    },
}

struct InboundWorker {
    /// Inbound socket
    inbound_socket: ZmqInboundSocket,

    /// Channel to broadcast received messages from socket
    rcv_tx: broadcast::Sender<TransportMessage>,

    /// Channel to communicate with outbound worker
    outbound_tx: mpsc::Sender<OutboundAction>,

    /// Shutdown token
    shutdown_rx: CancellationToken,
}

struct OutboundHandle {
    event_rx: BusEventStream,
}

struct OutboundWorker {
    /// zmq context
    context: zmq::Context,

    /// Configuration of the transport
    configuration: ZmqTransportConfiguration,

    /// Bus peer
    peer: Peer,

    /// Bus Environment
    environment: String,

    /// Map of zmq outbound socket to their corresponding peer id
    outbound_sockets: HashMap<PeerId, ZmqOutboundSocket>,

    /// Reception channel for outbound actions
    actions_rx: mpsc::Receiver<OutboundAction>,

    /// Stream of events from the bus
    event_rx: BusEventStream,

    /// Channel to receive incoming messages from
    /// Used to subscribe to receive incoming messages
    rcv_tx: tokio::sync::broadcast::Sender<TransportMessage>,

    /// Shutdown token
    shutdown_rx: CancellationToken,
}

trait DisconnectStrategy {
    fn disconnect(
        socket: &mut ZmqOutboundSocket,
        sender: &Peer,
        environment: &String,
        buf: &mut Vec<u8>,
    ) -> Result<(), ZmqError>;
}

struct EndStreamGracefully;
struct TerminateConnection;

impl DisconnectStrategy for EndStreamGracefully {
    fn disconnect(
        socket: &mut ZmqOutboundSocket,
        sender: &Peer,
        environment: &String,
        buf: &mut Vec<u8>,
    ) -> Result<(), ZmqError> {
        let peer = socket.peer().map_err(ZmqError::Outbound)?;
        debug!("sending EndOfStreamAck to {peer}");
        let (_, end_of_stream_ack) =
            TransportMessage::create(&sender, environment.clone(), &close::EndOfStreamAck {});
        buf.clear();
        end_of_stream_ack.encode(buf).map_err(ZmqError::Encode)?;
        socket.write_all(buf).map_err(ZmqError::Io)?;
        Ok(())
    }
}

impl DisconnectStrategy for TerminateConnection {
    fn disconnect(
        _socket: &mut ZmqOutboundSocket,
        _sender: &Peer,
        _environment: &String,
        _buf: &mut Vec<u8>,
    ) -> Result<(), ZmqError> {
        Ok(())
    }
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

    fn start(&mut self) -> Result<(), ZmqError> {
        info!("starting zmq transport...");

        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                options,
                peer_id,
                environment,
                event_rx,
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
                let mut inbound_endpoint = inbound_socket.bind().map_err(ZmqError::Inbound)?;
                let hostname = gethostname::gethostname();
                let host_str = hostname.to_str().ok_or(ZmqError::InvalidUtf8)?;
                inbound_endpoint = inbound_endpoint.replace("0.0.0.0", &host_str);
                info!("socket bound to endpoint {inbound_endpoint}");

                // Create cancellation token to shutdown inner components
                let cancel_rx = CancellationToken::new();
                let cancel_tx = CancellationToken::new();

                // Create broadcast channel for transport message reception
                let (rcv_tx, _rcv_rx) = broadcast::channel(128);

                let peer = Peer {
                    id: peer_id.clone(),
                    endpoint: inbound_endpoint.clone(),
                    is_up: true,
                    is_responding: true,
                };

                // Start outbound worker
                let (actions_tx, oubound_worker) = OutboundWorker::start(
                    context.clone(),
                    configuration.clone(),
                    peer,
                    environment.clone(),
                    event_rx,
                    rcv_tx.clone(),
                    cancel_tx.clone(),
                )?;

                // Start inbound worker
                let inbound_handle = InboundWorker::start(
                    inbound_socket,
                    rcv_tx.clone(),
                    actions_tx.clone(),
                    cancel_rx.clone(),
                )?;

                (
                    // Transition to Started state
                    Some(Inner::Started {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        inbound_endpoint,
                        inbound_handle,
                        outbound_handle: oubound_worker,
                        actions_tx,
                        rcv_tx,
                        shutdown_rx: cancel_rx,
                        shutdown_tx: cancel_tx,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(ZmqError::InvalidOperation)),
        };

        info!("... started");
        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<(), ZmqError> {
        info!("stopping zmq transport...");

        let (inner, res) = match self.inner.take() {
            Some(Inner::Started {
                configuration,
                options,
                inbound_handle,
                outbound_handle,
                shutdown_tx,
                shutdown_rx,
                ..
            }) => {
                // Shutdown outbound worker first.
                // We need to shutdown the oubound worker first because we still
                // need the inbound to receive `EndOfStreamAck` from peers
                shutdown_tx.cancel();

                // Wait for the inbound worker to stop;
                if let Err(_) = outbound_handle.join() {
                    error!("outbound worker panic'ed");
                }

                // Now, shutdown inbound worker
                shutdown_rx.cancel();
                // Wait for the inbound worker to stop;
                if let Err(_) = inbound_handle.join() {
                    error!("inbound worker panic'ed");
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
            x => (x, Err(ZmqError::InvalidOperation)),
        };

        info!("... stopped");
        self.inner = inner;
        res
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<(), ZmqError> {
        match self.inner.as_ref() {
            Some(Inner::Started { ref actions_tx, .. }) => {
                let peers = peers.collect();

                actions_tx
                    .try_send(OutboundAction::Send {
                        message,
                        peers,
                        context,
                    })
                    .expect("unexpected send failure");

                Ok(())
            }
            _ => Err(ZmqError::InvalidOperation),
        }
    }
}

impl InboundWorker {
    fn start(
        inbound_socket: ZmqInboundSocket,
        rcv_tx: broadcast::Sender<TransportMessage>,
        outbound_tx: mpsc::Sender<OutboundAction>,
        shutdown_rx: CancellationToken,
    ) -> Result<JoinHandle<()>, ZmqError> {
        // Create inbound worker
        let worker = InboundWorker {
            inbound_socket,
            rcv_tx,
            outbound_tx,
            shutdown_rx,
        };

        // Create tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ZmqError::Io)?;

        // Spawn inbound thread
        let inbound_thread = std::thread::Builder::new()
            .name(INBOUND_THREAD_NAME.into())
            .spawn(move || rt.block_on(worker.run()))
            .map_err(ZmqError::Io)?;

        Ok(inbound_thread)
    }

    #[cfg(unix)]
    async fn run(mut self) {
        use crate::core::MessagePayload;

        self.inbound_socket.enable_polling().unwrap();

        let mut rcv_buf = [0u8; 4096];

        loop {
            tokio::select! {
                _ = self.shutdown_rx.cancelled() => { break },
                Ok(size) = self.inbound_socket.read(&mut rcv_buf[..]) => {
                    match TransportMessage::decode(&rcv_buf[..size]) {
                        Ok(message) =>  {
                            // If we received an `EndOfStream` from a peer, send
                            // message to outbound worker to end the stream of the peer
                            if message.is::<EndOfStream>() {
                                let peer_id = message.originator.sender_id;
                                if let Err(e) = self.outbound_tx.send(OutboundAction::EndStream { peer_id }).await {
                                    let peer_id = if let OutboundAction::EndStream { peer_id } = e.0 { peer_id } else { unreachable!() };
                                    warn!("Could not end stream of {peer_id}: channel has ben closed");
                                }

                            } else {
                                let _ = self.rcv_tx.send(message);
                            }
                        },
                        Err(e) => error!("Failed to decode: {e}. bytes {rcv_buf:?}"),
                    };
                }
            }
        }

        let _ = self.close();
        debug!("inbound stopped");
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

    fn close(&mut self) -> Result<(), ZmqError> {
        if let Err(e) = self.inbound_socket.close() {
            warn!("failed to unbind: {e}");
        }

        Ok(())
    }
}

impl OutboundWorker {
    fn start(
        context: zmq::Context,
        configuration: ZmqTransportConfiguration,
        peer: Peer,
        environment: String,
        event_rx: BusEventStream,
        rcv_tx: tokio::sync::broadcast::Sender<TransportMessage>,
        shutdown_rx: CancellationToken,
    ) -> Result<
        (
            mpsc::Sender<OutboundAction>,
            JoinHandle<Result<OutboundHandle, ZmqError>>,
        ),
        ZmqError,
    > {
        // Create outbound channel for outbound socket operations
        // TODO(oktal): remove hardcoded bound limit
        let (actions_tx, actions_rx) = mpsc::channel(128);

        // Create outbound worker
        let worker = OutboundWorker {
            context,
            configuration,
            peer,
            environment,
            outbound_sockets: HashMap::new(),
            actions_rx,
            event_rx,
            rcv_tx,
            shutdown_rx,
        };

        // Create tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ZmqError::Io)?;

        // Spawn outbound thread
        let outbound_thread = std::thread::Builder::new()
            .name(OUTBOUND_THREAD_NAME.into())
            .spawn(move || rt.block_on(worker.run()))
            .map_err(ZmqError::Io)?;

        Ok((actions_tx, outbound_thread))
    }

    async fn run(mut self) -> Result<OutboundHandle, ZmqError> {
        let mut encode_buf = vec![0u8; 1024];

        loop {
            let (action_name, res) = tokio::select! {
                _ = self.shutdown_rx.cancelled() => { break; },
                Some(action) = self.actions_rx.recv() => {
                    (action.as_str(), self.handle_action(action, &mut encode_buf))
                },
                Some(event) = self.event_rx.next() => {
                    let kind = match &event {
                        BusEvent::Starting => "BusStarting",
                        BusEvent::Registering(_) => "BusRegistering",
                        BusEvent::Registered(_) => "BusRegistered",
                        BusEvent::Started => "BusStarted",
                        BusEvent::Stopping => "BusStopping",
                        BusEvent::Stopped => "BusStopped",
                        BusEvent::Peer(ev) => ev.kind().as_str(),
                    };
                    (kind,  self.handle_event(event, &mut encode_buf))
                },
            };

            if let Err(e) = res {
                warn!("failed to handle {action_name}: {e}");
            }
        }

        if let Err(e) = self.close_all().await {
            warn!("failed to stop inbound: {e}");
        }

        self.disconnect_all();

        debug!("outbound stopped");

        Ok(OutboundHandle {
            event_rx: self.event_rx,
        })
    }

    fn handle_action(
        &mut self,
        action: OutboundAction,
        encode_buf: &mut Vec<u8>,
    ) -> Result<(), ZmqError> {
        match action {
            OutboundAction::Send {
                message,
                peers,
                context,
            } => self.handle_send(message, peers, context, encode_buf),
            OutboundAction::EndStream { peer_id } => {
                self.disconnect::<EndStreamGracefully>(&peer_id, encode_buf)
            }
        }
    }

    fn handle_event(&mut self, event: BusEvent, encode_buf: &mut Vec<u8>) -> Result<(), ZmqError> {
        if let BusEvent::Peer(peer_event) = event {
            if peer_event.peer_id() == &self.peer.id {
                return Ok(());
            }

            match peer_event {
                PeerEvent::Decomissionned(peer) if !peer.id.is_persistence() => {
                    self.disconnect::<TerminateConnection>(&peer.id, encode_buf)
                }
                // If a previously existing peer starts up with a new endpoint, make sure to disconnect
                // the previous socket to avoid keeping stale sockets
                PeerEvent::Started(peer) => {
                    self.disconnect::<TerminateConnection>(&peer.id, encode_buf)
                }
                _ => Ok(()),
            }
        } else {
            Ok(())
        }
    }

    fn handle_send(
        &mut self,
        mut message: TransportMessage,
        peers: Vec<Peer>,
        context: SendContext,
        encode_buf: &mut Vec<u8>,
    ) -> Result<(), ZmqError> {
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
    ) -> Result<(), ZmqError> {
        let socket = self.get_connected_socket(&peer)?;
        let bytes = Self::encode(message, encode_buf)?;

        socket.write_all(bytes).map_err(ZmqError::Io)
    }

    fn get_socket(&mut self, peer_id: &PeerId) -> Result<&mut ZmqOutboundSocket, ZmqError> {
        let socket = self
            .outbound_sockets
            .entry(peer_id.clone())
            .or_insert_with_key(|peer_id| {
                ZmqOutboundSocket::new(self.context.clone(), peer_id.clone())
            });
        Ok(socket)
    }

    fn get_connected_socket(&mut self, peer: &Peer) -> Result<&mut ZmqOutboundSocket, ZmqError> {
        let socket = self.get_socket(&peer.id)?;
        // TODO(oktal): Handle reconnection to a different endpoint
        if !socket.is_connected() {
            socket.connect(&peer.endpoint).map_err(ZmqError::Outbound)?;
            info!("connected to {peer}");
        }
        Ok(socket)
    }

    fn disconnect<S>(&mut self, peer_id: &PeerId, encode_buf: &mut Vec<u8>) -> Result<(), ZmqError>
    where
        S: DisconnectStrategy,
    {
        info!("disconnecting peer {peer_id}");
        if let Some(mut socket) = self.outbound_sockets.remove(&peer_id) {
            // Invoke the disconnect strategy
            S::disconnect(&mut socket, &self.peer, &self.environment, encode_buf)?;

            // Disconnect the underlying zmq socket
            socket.disconnect().map_err(ZmqError::Outbound)?;
            // Dropping the socket will close the underlying zmq file descriptor
            drop(socket);

            Ok(())
        } else {
            Err(ZmqError::UnknownPeer(peer_id.clone()))
        }
    }

    async fn close_all(&mut self) -> Result<(), ZmqError> {
        let timeout =
            tokio::time::Duration::from(self.configuration.wait_for_end_of_stream_ack_timeout);

        for (peer_id, mut socket) in &mut self.outbound_sockets {
            let endpoint = socket.endpoint().unwrap_or("NA").to_string();

            info!("sending EndOfStream to peer {peer_id} [{endpoint}] ...");

            let future = super::close::close(
                &self.peer,
                self.environment.clone(),
                &mut socket,
                self.rcv_tx.subscribe(),
            )?;

            match tokio::time::timeout(timeout, future).await {
                Ok(elapsed) => {
                    info!("... received EndOfStreamAck from {peer_id} [{endpoint}] in {elapsed:?}")
                }
                Err(_) => warn!("... did not receive EndOfStreamAck from {peer_id} [{endpoint}] after {timeout:?}"),
            }
        }

        Ok(())
    }

    fn disconnect_all(&mut self) {
        for (peer_id, mut socket) in &mut self.outbound_sockets.drain() {
            let endpoint = socket.endpoint().unwrap_or("NA").to_string();

            info!("disconnecting from peer {peer_id} [{endpoint}] ...");

            match socket.disconnect() {
                Ok(()) => info!("... disconnected from peer {peer_id} [{endpoint}]"),
                Err(e) => warn!("failed to disconnect from peer {peer_id} [{endpoint}]: {e}"),
            }
        }
    }

    fn encode<'a>(
        message: &TransportMessage,
        encode_buf: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], ZmqError> {
        encode_buf.clear();
        message.encode(encode_buf).map_err(ZmqError::Encode)?;

        Ok(&encode_buf[..])
    }
}

impl Transport for ZmqTransport {
    type Err = ZmqError;
    type MessageStream = super::MessageStream;

    type StartCompletionFuture = futures_util::future::Ready<Result<(), Self::Err>>;
    type StopCompletionFuture = futures_util::future::Ready<Result<(), Self::Err>>;
    type SendFuture = transport::future::SendFuture<OutboundAction, Self::Err>;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        event_rx: EventStream<BusEvent>,
    ) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Unconfigured {
                configuration,
                options,
            }) => {
                let event_rx = event_rx.boxed();

                // Transition to Configured state
                (
                    Some(Inner::Configured {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        event_rx,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(ZmqError::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
        match self.inner {
            Some(Inner::Started { ref rcv_tx, .. }) => Ok(rcv_tx.subscribe().into()),
            _ => Err(ZmqError::InvalidOperation),
        }
    }

    fn start(&mut self) -> Result<Self::StartCompletionFuture, Self::Err> {
        self.start()?;
        Ok(futures_util::future::ready(Ok(())))
    }

    fn stop(&mut self) -> Result<Self::StopCompletionFuture, Self::Err> {
        self.stop()?;
        Ok(futures_util::future::ready(Ok(())))
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
            _ => Err(ZmqError::InvalidOperation),
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
            _ => Err(ZmqError::InvalidOperation),
        }
    }

    fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref inbound_endpoint,
                ..
            }) => Ok(Cow::Borrowed(inbound_endpoint.as_str())),
            _ => Err(ZmqError::InvalidOperation),
        }
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<Self::SendFuture, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { ref actions_tx, .. }) => {
                Ok(SendFuture::new(actions_tx.clone(), peers, message))
            }
            _ => Err(ZmqError::InvalidOperation),
        }
    }
}
