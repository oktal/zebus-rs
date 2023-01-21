use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use crate::{
    bus::{CommandFuture, CommandResult},
    bus_configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    core::MessagePayload,
    directory::{
        self, commands::PingPeerCommand, events::PeerSubscriptionsForTypeUpdated, Directory,
        PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
        Registration,
    },
    dispatch::{
        self, AnyMessage, DispatchOutput, DispatchRequest, Dispatched, Dispatcher,
        MessageDispatcher,
    },
    proto::{FromProtobuf, prost},
    transport::{MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    BusConfiguration, Command, Event, Handler, MessageId, Peer, PeerId,
};

#[derive(Debug)]
pub struct RegistrationError {
    inner: Vec<(Peer, directory::RegistrationError)>,
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "failed to register to directory:")?;
        for failure in &self.inner {
            writeln!(f, "    tried {}: {}", failure.0, failure.1)?;
        }

        Ok(())
    }
}

impl RegistrationError {
    fn new() -> Self {
        Self { inner: vec![] }
    }

    fn add(&mut self, peer: Peer, error: directory::RegistrationError) {
        self.inner.push((peer, error))
    }

    fn find(
        &self,
        predicate: impl Fn(&directory::RegistrationError) -> bool,
    ) -> Option<&directory::RegistrationError> {
        self.inner.iter().find(|e| predicate(&e.1)).map(|x| &x.1)
    }
}

#[derive(Debug, Error)]
pub enum SendError {
    /// An attempt to send a [`Command`] resulted in no candidat peer
    #[error("unable to find peer for command")]
    NoPeer,

    /// An attempt to send a [`Command`] resulted in multiple candidate peers
    #[error("can not send a command to multiple peers: {0:?}")]
    MultiplePeers(Vec<Peer>),

    /// A [`Command`] could not be send to a non responding [`Peer`]
    #[error("can not send a transient message to non responding peer {0:?}")]
    PeerNotResponding(Peer),

    /// The sender has been closed
    #[error("sender has been closed")]
    Closed,
}

#[derive(Debug, Error)]
pub enum Error {
    /// Transport error
    #[error("an error occured during a transport operation {0}")]
    Transport(Box<dyn std::error::Error>),

    /// None of the directories tried for registration succeeded
    #[error("{0}")]
    Registration(RegistrationError),

    /// An error occured when sending a message to one or multiple peers
    #[error("{0}")]
    Send(SendError),

    /// An error occured on the dispatcher
    #[error("an error occured on the dispatcher {0}")]
    Dispatch(dispatch::Error),

    /// An operation was attempted while the [`Bus`] was in an invalid state
    #[error("an operation was attempted while the bus was not in a valid state")]
    InvalidOperation,
}

enum State<T: Transport, D: Directory> {
    Init {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        transport: T,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    },

    Configured {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        transport: T,
        dispatcher: MessageDispatcher,

        directory: Arc<D>,
        peer_id: PeerId,
        environment: String,
    },

    Started {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        self_peer: Peer,
        environment: String,

        directory: Arc<D>,

        pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
        snd_tx: mpsc::Sender<SendEntry>,
        dispatch_tx: mpsc::Sender<LocalDispatchRequest>,
        rx_handle: JoinHandle<()>,
        tx_handle: JoinHandle<()>,
    },
}

struct SendEntry {
    message: TransportMessage,
    peers: Vec<Peer>,
}

struct SenderContext<T: Transport> {
    transport: T,
    rx: mpsc::Receiver<SendEntry>,
}

impl<T: Transport> SenderContext<T> {
    fn new(transport: T) -> (mpsc::Sender<SendEntry>, Self) {
        // TODO(oktal): hardcoded limit
        let (tx, rx) = mpsc::channel(128);
        (tx, Self { transport, rx })
    }

    fn send<M: crate::Message + prost::Message>(
        message: &M,
        snd_tx: &mpsc::Sender<SendEntry>,
        self_peer: &Peer,
        environment: String,
        pending_commands: &mut HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>,
        peers: impl IntoIterator<Item = Peer>,
    ) -> Result<CommandFuture, Error> {
        let (tx, rx) = oneshot::channel();
        let (id, message) = TransportMessage::create(&self_peer, environment.clone(), message);

        pending_commands.entry(id).or_insert(tx);

        // TODO(oktal): use async send
        let _ = snd_tx.blocking_send(SendEntry {
            message,
            peers: peers.into_iter().collect(),
        });
        Ok(CommandFuture(rx))
    }

    fn handle_send(&mut self, entry: SendEntry) {
        self.transport.send(
            entry.peers.into_iter(),
            entry.message,
            SendContext::default(),
        );
    }
}

/// A [`Message`] to be dispatched locally
enum LocalDispatchRequest {
    /// A [`Command`] to be dispatched locally
    Command {
        tx: oneshot::Sender<CommandResult>,
        message_type: &'static str,
        message: Arc<AnyMessage>,
    },

    /// An [`Event`] to be dispatched locally
    Event {
        message_type: &'static str,
        message: Arc<AnyMessage>,
    },
}

impl LocalDispatchRequest {
    /// Create a [`LocalDispatchRequest`] for a [`Command`] message
    fn for_command<C>(command: C) -> (Self, oneshot::Receiver<CommandResult>)
    where
        C: crate::Message + prost::Message + Command + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let request = Self::Command {
            tx,
            message_type: C::name(),
            message: Arc::new(command),
        };

        (request, rx)
    }

    /// Create a [`LocalDispatchRequest`] for a [`Event`] message
    fn for_event<E>(event: E) -> Self
    where
        E: crate::Message + prost::Message + Event + Send + 'static,
    {
        Self::Event {
            message_type: E::name(),
            message: Arc::new(event),
        }
    }
}

struct ReceiverContext<T: Transport, D: Directory> {
    rcv_rx: T::MessageStream,
    dispatch_rx: mpsc::Receiver<LocalDispatchRequest>,
    tx: mpsc::Sender<SendEntry>,
    self_peer: Peer,
    environment: String,
    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    directory: Arc<D>,
    dispatcher: MessageDispatcher,
}

impl<T: Transport, D: Directory> ReceiverContext<T, D> {
    fn new(
        transport_rx: T::MessageStream,
        tx: mpsc::Sender<SendEntry>,
        self_peer: Peer,
        environment: String,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    ) -> (
        Self,
        mpsc::Sender<LocalDispatchRequest>,
        Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    ) {
        let pending_commands = Arc::new(Mutex::new(HashMap::new()));
        let (dispatch_tx, dispatch_rx) = mpsc::channel(128);

        (
            Self {
                rcv_rx: transport_rx,
                dispatch_rx,
                tx,
                self_peer,
                environment,
                pending_commands: pending_commands.clone(),
                directory,
                dispatcher,
            },
            dispatch_tx,
            pending_commands,
        )
    }

    async fn dispatch(&mut self, request: DispatchRequest) -> Dispatched {
        self.dispatcher.dispatch(request).await
    }

    async fn send<M: prost::Message + crate::Message>(&mut self, message: &M) {
        let dst_peers = self.directory.get_peers_handling(message);
        if !dst_peers.is_empty() {
            self.send_to(message, dst_peers).await;
        }
    }

    async fn send_to<M: prost::Message + crate::Message>(&mut self, message: &M, peers: Vec<Peer>) {
        let (_id, message) =
            TransportMessage::create(&self.self_peer, self.environment.clone(), message);

        let _ = self.tx.send(SendEntry { message, peers }).await;
    }
}

/// Reception loop for [`TransportMesssage`] messages
async fn receiver<T: Transport, D: Directory>(mut ctx: ReceiverContext<T, D>) {
    loop {
        tokio::select! {

            // Handle inbound TransportMessage
            Some(message) = ctx.rcv_rx.next() => {
                // Handle MessageExecutionCompleted
                if let Some(message_execution_completed) = message.decode_as::<MessageExecutionCompleted>()
                {
                    // TODO(oktal): do not silently ignore error
                    if let Ok(message_execution_completed) = message_execution_completed {
                        // Get the orignal command MessageId
                        let command_id =
                            MessageId::from_protobuf(message_execution_completed.command_id.clone());

                        // Retrieve the pending command associated with the MessageExecutionCompleted
                        // TODO(oktal): do not silently ignore when failing to find the pending command
                        let mut pending_commands = ctx.pending_commands.lock().unwrap();
                        if let Some(pending_command_tx) = pending_commands.remove(&command_id.value()) {
                            // Resolve the command with the execution result
                            let _ = pending_command_tx.send(message_execution_completed.into());
                        }
                    }
                }
                // Handle message
                else {
                    // Dispatch message
                    let dispatched = ctx.dispatch(DispatchRequest::Remote(message)).await;

                    // Retrieve the output of the dispatch
                    let output: DispatchOutput = dispatched.into();

                    // If the message that has been dispatched is a Command, send back the
                    // MessageExecutionCompleted
                    if let Some(completed) = output.completed {
                        ctx.send_to(
                            &completed,
                            vec![output.originator.expect("missing originator")],
                        )
                        .await;
                    }

                    // Publish [`MessageProcessingFailed`] if some handlers failed
                    if let Some(failed) = output.failed {
                        ctx.send(&failed).await;
                    }
                }
            },

            // Handle local message dispatch request
            Some(request) = ctx.dispatch_rx.recv() => {
                // TODO(oktal): send `MessageProcessingFailed` if dispatch failed ?
                match request {
                    LocalDispatchRequest::Command { tx, message_type, message } => {
                        // Dispatch command locally
                        let dispatched = ctx.dispatch(DispatchRequest::Local(message_type, message)).await;

                        // Retrieve the `CommandResult`
                        let command_result: Option<CommandResult> = dispatched.try_into().ok();

                        // Resolve command future
                        if let Some(command_result) = command_result {
                            let _ = tx.send(command_result);
                        }
                    },
                    LocalDispatchRequest::Event { message_type, message } => {
                        // Dispatch event locally
                        let _dispatched = ctx.dispatch(DispatchRequest::Local(message_type, message)).await;
                    }
                }
            }
        }
    }
}

/// Sender loop for [`TransportMessage`] messages
async fn sender<T: Transport>(mut ctx: SenderContext<T>) {
    while let Some(entry) = ctx.rx.recv().await {
        ctx.handle_send(entry);
    }
}

/// Register to the directory
async fn register<T: Transport>(
    transport: &mut T,
    self_peer: &Peer,
    environment: &String,
    configuration: &BusConfiguration,
) -> Result<Registration, Error> {
    let directory_peers =
        configuration
            .directory_endpoints
            .iter()
            .enumerate()
            .map(|(idx, endpoint)| {
                let peer_id = PeerId::directory(idx);

                Peer {
                    id: peer_id,
                    endpoint: endpoint.to_string(),
                    is_up: true,
                    is_responding: true,
                }
            });

    let timeout = configuration.registration_timeout;
    let mut error = RegistrationError::new();
    for directory_peer in directory_peers {
        match directory::registration::register(
            transport,
            self_peer.clone(),
            environment.clone(),
            directory_peer.clone(),
            timeout,
        )
        .await
        {
            Ok(r) => return Ok(r),
            Err(e) => error.add(directory_peer.clone(), e),
        }
    }

    Err(Error::Registration(error))
}

struct Bus<T: Transport, D: Directory> {
    inner: Option<State<T, D>>,
}

impl<T: Transport, D: Directory> Bus<T, D> {
    fn new(
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        transport: T,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    ) -> Self {
        Self {
            inner: Some(State::Init {
                runtime,
                configuration,
                transport,
                directory,
                dispatcher,
            }),
        }
    }
}

impl<T: Transport, D: Directory> Bus<T, D> {
    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(State::Init {
                runtime,
                configuration,
                mut transport,
                directory,
                dispatcher,
            }) => {
                // Subscribe to the directory event stream
                let directory_rx = directory.subscribe();

                // Pin the directory event stream
                let directory_rx = Box::pin(directory_rx);

                // Configure transport
                transport
                    .configure(
                        peer_id.clone(),
                        environment.clone(),
                        directory_rx,
                        Arc::clone(&runtime),
                    )
                    .map_err(|e| Error::Transport(e.into()))?;

                (
                    Some(State::Configured {
                        runtime,
                        configuration,
                        transport,
                        dispatcher,
                        directory,
                        peer_id,
                        environment,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn start(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(State::Configured {
                runtime,
                configuration,
                mut transport,
                mut dispatcher,
                peer_id,
                directory,
                environment,
            }) => {
                // Start transport
                transport.start().map_err(|e| Error::Transport(e.into()))?;

                // Setup peer
                let endpoint = transport
                    .inbound_endpoint()
                    .map_err(|e| Error::Transport(e.into()))?;

                let self_peer = Peer {
                    id: peer_id.clone(),
                    endpoint: endpoint.to_string(),
                    is_up: true,
                    is_responding: true,
                };

                // Register to directory
                let registration = runtime.block_on(async {
                    register(&mut transport, &self_peer, &environment, &configuration).await
                })?;

                let mut directory_handler = directory.handler();

                // Handle peer directory response
                if let Ok(response) = registration.result {
                    directory_handler.handle(response);
                }

                // Setup peer directory client handler
                dispatcher
                    .add(dispatch::registry::for_handler(directory_handler, |h| {
                        h.handles::<PeerStarted>()
                            .handles::<PeerStopped>()
                            .handles::<PeerDecommissioned>()
                            .handles::<PeerNotResponding>()
                            .handles::<PeerResponding>()
                            .handles::<PingPeerCommand>()
                            .handles::<PeerSubscriptionsForTypeUpdated>();
                    }))
                    .map_err(Error::Dispatch)?;

                // Start the dispatcher
                dispatcher.start().map_err(Error::Dispatch)?;

                // Create transport reception stream
                let rcv_rx = transport
                    .subscribe()
                    .map_err(|e| Error::Transport(e.into()))?;

                // Create sender
                let (snd_tx, snd_ctx) = SenderContext::new(transport);

                // Create receiver
                let (rcv_ctx, dispatch_tx, pending_commands) = ReceiverContext::<T, D>::new(
                    rcv_rx,
                    snd_tx.clone(),
                    self_peer.clone(),
                    environment.clone(),
                    directory.clone(),
                    dispatcher,
                );

                // Start sender
                let tx_handle = runtime.spawn(sender(snd_ctx));

                // Start receiver
                let rx_handle = runtime.spawn(receiver(rcv_ctx));

                // Transition to started state
                (
                    Some(State::Started {
                        runtime,
                        configuration,
                        self_peer,
                        environment,
                        directory,
                        pending_commands,
                        snd_tx,
                        dispatch_tx,
                        rx_handle,
                        tx_handle,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<(), Error> {
        todo!()
    }

    fn send<C: Command + prost::Message + Send + 'static>(
        &mut self,
        command: C,
    ) -> Result<CommandFuture, Error> {
        match self.inner.as_mut() {
            Some(State::Started {
                ref snd_tx,
                ref pending_commands,
                ref self_peer,
                ref directory,
                ref environment,
                ref dispatch_tx,
                ..
            }) => {
                // Retrieve the list of peers handling the command from the directory
                let peers = directory.get_peers_handling(&command);

                let self_dst_peer = peers.iter().find(|&p| p.id == self_peer.id);
                // TODO(oktal): add local dispatch toggling
                // If we are the receiver of the command, do a local dispatch
                if self_dst_peer.is_some() {
                    // Create the local dispatch request for the command
                    let (request, rx) = LocalDispatchRequest::for_command(command);

                    // Send the command
                    // TODO(oktal): use async send
                    dispatch_tx
                        .blocking_send(request)
                        .map_err(|_| Error::Send(SendError::Closed))?;

                    // Return the future
                    Ok(CommandFuture(rx))
                } else {
                    // Make sure there is only one peer handling the command
                    let dst_peer = peers
                        .into_iter()
                        .at_most_one()
                        .map_err(|e| Error::Send(SendError::MultiplePeers(e.collect())))?
                        .ok_or(Error::Send(SendError::NoPeer))?;

                    // Attempting to send a non-persistent command to a non-responding peer result in
                    // an error
                    if !dst_peer.is_responding && C::TRANSIENT && !C::INFRASTRUCTURE {
                        Err(Error::Send(SendError::PeerNotResponding(dst_peer)))
                    } else {
                        // Lock the map of pending commands
                        let mut pending_commands = pending_commands.lock().unwrap();

                        // Send the command
                        SenderContext::<T>::send(
                            &command,
                            snd_tx,
                            self_peer,
                            environment.clone(),
                            &mut pending_commands,
                            std::iter::once(dst_peer),
                        )
                    }
                }
            }
            _ => Err(Error::InvalidOperation),
        }
    }

    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: C,
        peer: crate::Peer,
    ) -> Result<CommandFuture, Error> {
        match self.inner.as_mut() {
            Some(State::Started {
                ref snd_tx,
                ref pending_commands,
                ref self_peer,
                ref environment,
                ..
            }) => {
                // Attempting to send a non-persistent command to a non-responding peer result in
                // an error
                if !peer.is_responding && C::TRANSIENT && !C::INFRASTRUCTURE {
                    return Err(Error::Send(SendError::PeerNotResponding(peer)));
                }

                // Lock the map of pending commands
                let mut pending_commands = pending_commands.lock().unwrap();

                // Send the command
                SenderContext::<T>::send(
                    &command,
                    snd_tx,
                    self_peer,
                    environment.clone(),
                    &mut pending_commands,
                    std::iter::once(peer),
                )
            }
            _ => Err(Error::InvalidOperation),
        }
    }
}

impl<T: Transport, D: Directory> crate::Bus for Bus<T, D> {
    type Err = Error;

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err> {
        self.configure(peer_id, environment)
    }

    fn start(&mut self) -> Result<(), Self::Err> {
        self.start()
    }

    fn stop(&mut self) -> Result<(), Self::Err> {
        self.stop()
    }

    fn send<C: Command + prost::Message + Send + 'static>(
        &mut self,
        command: C,
    ) -> Result<CommandFuture, Self::Err> {
        self.send(command)
    }

    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: C,
        peer: Peer,
    ) -> Result<CommandFuture, Self::Err> {
        self.send_to(command, peer)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CreateError<E> {
    MissingConfiguration,

    Configure(E),
}

pub struct BusBuilder<T: Transport> {
    transport: T,
    peer_id: Option<PeerId>,
    configuration: Option<BusConfiguration>,
    environment: Option<String>,
    runtime: Option<Runtime>,
    dispatcher: MessageDispatcher,
}

impl<T: Transport> BusBuilder<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            peer_id: None,
            configuration: None,
            environment: None,
            runtime: None,
            dispatcher: MessageDispatcher::new(),
        }
    }

    pub fn with_runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn with_configuration(
        mut self,
        configuration: BusConfiguration,
        environment: String,
    ) -> Self {
        self.configuration = Some(configuration);
        self.environment = Some(environment);
        self
    }

    pub fn with_default_configuration(
        self,
        directory_endpoints: &str,
        environment: String,
    ) -> Self {
        let directory_endpoints = directory_endpoints
            .split(&[' ', ',', ';'])
            .map(Into::into)
            .collect();
        let configuration = BusConfiguration {
            directory_endpoints,
            registration_timeout: DEFAULT_REGISTRATION_TIMEOUT,
            start_replay_timeout: DEFAULT_START_REPLAY_TIMEOUT,
            is_persistent: false,
            pick_random_directory: false,
            enable_error_publication: false,
            message_batch_size: DEFAULT_MAX_BATCH_SIZE,
        };
        self.with_configuration(configuration, environment)
    }

    pub fn with_handler<H>(
        mut self,
        handler: Box<H>,
        registry_fn: impl FnOnce(&mut dispatch::registry::Registry<H>),
    ) -> Self
    where
        H: crate::DispatchHandler + Send + 'static,
    {
        let registry = dispatch::registry::for_handler(handler, registry_fn);
        self.dispatcher.add(registry).unwrap();
        self
    }

    pub fn create(self) -> Result<impl crate::Bus, CreateError<Error>> {
        let (transport, peer_id, configuration, environment) = (
            self.transport,
            self.peer_id.unwrap_or(Self::testing_peer_id()),
            self.configuration
                .ok_or(CreateError::MissingConfiguration)?,
            self.environment.ok_or(CreateError::MissingConfiguration)?,
        );

        // Create tokio runtime
        let runtime = self.runtime.unwrap_or(Self::default_runtime());

        let dispatcher = self.dispatcher;

        // Create peer directory client
        let client = directory::Client::new();

        // Create the bus
        let mut bus = Bus::new(
            Arc::new(runtime),
            configuration,
            transport,
            client,
            dispatcher,
        );

        // Configure the bus
        bus.configure(peer_id, environment)
            .map_err(CreateError::Configure)?;
        Ok(bus)
    }

    fn default_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    fn testing_peer_id() -> PeerId {
        let uuid = uuid::Uuid::new_v4();
        let peer_id = format!("Abc.Testing.{uuid}");
        PeerId::new(peer_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, borrow::Cow, time::Duration};

    use super::*;
    use crate::bus::CommandError;
    use crate::message_type_id::MessageTypeId;
    use crate::{
        directory::{
            commands::{RegisterPeerCommand, RegisterPeerResponse},
            event::PeerEvent,
            DirectoryReader,
        },
        dispatch::registry,
    };
    use tokio::sync::broadcast;

    /// Inner [`MemoryTransport`] state
    struct MemoryTransportInner {
        /// Configured peer id
        peer_id: Option<PeerId>,

        /// Configured environment
        environment: Option<String>,

        /// Flag indicating whether the transport has been started
        started: bool,

        /// Sender channel for transport messages
        rcv_tx: Option<broadcast::Sender<TransportMessage>>,

        /// Transmit queue
        /// Messages that are sent through the transport will be stored in this queue along with
        /// the recipient peers
        tx_queue: Vec<(TransportMessage, Vec<Peer>)>,

        /// Reception queue
        /// Messages that should be "sent" back as a response to a transport message will be stored
        /// in this queue.
        ///
        /// This queue holds two callbacks:
        /// First callback is a predicate that will be used to determine whether the transport
        /// should respond to a particular message
        /// Second callback will be used to create an instance of a transport message that should
        /// be sent back
        rx_queue: Vec<(
            Box<dyn Fn(&TransportMessage, &Peer) -> bool + Send + 'static>,
            Box<dyn FnOnce(TransportMessage, Peer, String) -> TransportMessage + Send + 'static>,
        )>,
    }

    /// A [`Transport`] that stores state in memory and has simplified logic for test purposes
    struct MemoryTransport {
        /// The peer the transport is operating on
        peer: Peer,
        /// Shared transport state
        inner: Arc<Mutex<MemoryTransportInner>>,
    }

    /// Inner [`MemoryDirectory`] state
    struct MemoryDirectoryInner {
        /// Sender channel for peer events
        events_tx: broadcast::Sender<PeerEvent>,

        /// Collection of messages that have been handled by the directory, indexed by their
        /// message type
        messages: HashMap<&'static str, Vec<Arc<dyn Any + Send + Sync>>>,

        /// Collection of peers that have been configured to handle a type of message
        peers: HashMap<&'static str, Vec<Peer>>,
    }

    impl MemoryDirectoryInner {
        fn new() -> Self {
            let (events_tx, _events_rx) = broadcast::channel(128);
            Self {
                events_tx,
                messages: HashMap::new(),
                peers: HashMap::new(),
            }
        }

        fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
            self.events_tx.subscribe()
        }

        fn add_peer_for<M: crate::Message>(&mut self, peer: Peer) {
            self.peers.entry(M::name()).or_insert(vec![]).push(peer);
        }
    }

    /// A [`Directory`] that stores state in memory and has simplified
    /// logic for test purposes
    struct MemoryDirectory {
        inner: Arc<Mutex<MemoryDirectoryInner>>,
    }

    impl MemoryDirectory {
        /// Get a list of messages handled by the directory
        fn get_handled<M: crate::Message + Send + Sync + 'static>(&self) -> Vec<Arc<M>> {
            let inner = self.inner.lock().unwrap();

            let name = M::name();
            match inner.messages.get(name) {
                Some(entry) => entry
                    .iter()
                    .filter_map(|m| m.clone().downcast::<M>().ok())
                    .collect(),
                None => vec![],
            }
        }

        /// Add a peer that should hande the `Message`
        fn add_peer_for<M: crate::Message>(&self, peer: Peer) {
            let mut inner = self.inner.lock().unwrap();
            inner.add_peer_for::<M>(peer);
        }
    }

    #[derive(Handler)]
    struct MemoryDirectoryHandler {
        inner: Arc<Mutex<MemoryDirectoryInner>>,
    }

    impl MemoryDirectoryHandler {
        /// Ad a `message` to the list of handled messages by the directory
        fn add_handled<M: crate::Message + Send + Sync + 'static>(&mut self, message: M) {
            let mut inner = self.inner.lock().unwrap();
            let name = M::name();
            let entry = inner.messages.entry(name).or_insert(Vec::new());
            entry.push(Arc::new(message));
        }
    }

    #[derive(Debug, Error)]
    enum MemoryTransportError {
        #[error("invalid operation")]
        InvalidOperation,
    }

    impl MemoryTransport {
        fn new(peer: Peer) -> Self {
            Self {
                peer,
                inner: Arc::new(Mutex::new(MemoryTransportInner {
                    peer_id: None,
                    environment: None,
                    started: false,
                    rcv_tx: None,
                    tx_queue: Vec::new(),
                    rx_queue: Vec::new(),
                })),
            }
        }

        fn is_started(&self) -> bool {
            let inner = self.inner.lock().unwrap();
            inner.started
        }

        /// Queue a message that will be sent back as a response to a transport message
        fn queue_message<M: crate::Message + prost::Message>(
            &self,
            predicate: impl Fn(&TransportMessage, &Peer) -> bool + Send + 'static,
            message_fn: impl Fn(TransportMessage) -> M + Send + Sync + 'static,
        ) -> Option<()> {
            let message_fn = Box::new(message_fn);
            let create_fn = Box::new(move |transport_message, sender, environment| {
                let message = message_fn(transport_message);
                let (_id, transport) = TransportMessage::create(&sender, environment, &message);
                transport
            });

            let mut inner = self.inner.lock().unwrap();
            inner.rx_queue.push((Box::new(predicate), create_fn));
            Some(())
        }

        fn message_received<M: crate::Message + prost::Message>(
            &self,
            message: M,
            sender: &Peer,
            environment: String,
        ) -> Option<()> {
            let inner = self.inner.lock().unwrap();
            let rcv_tx = inner.rcv_tx.as_ref()?;
            let (_id, transport) = TransportMessage::create(sender, environment, &message);
            rcv_tx.send(transport).ok()?;
            Some(())
        }

        /// Get the list of messages that have been sent through the transport
        fn get<M: crate::Message + prost::Message + Default>(&self) -> Vec<(M, Vec<Peer>)> {
            let inner = self.inner.lock().unwrap();
            inner
                .tx_queue
                .iter()
                .filter_map(|(msg, peers)| {
                    let message = msg.decode_as::<M>()?.ok()?;
                    Some((message, peers.clone()))
                })
                .collect()
        }

        /// Get the configured peer id
        fn get_peer_id(&self) -> Option<PeerId> {
            let inner = self.inner.lock().unwrap();
            inner.peer_id.clone()
        }

        /// Get the configured environment
        fn get_environment(&self) -> Option<String> {
            let inner = self.inner.lock().unwrap();
            inner.environment.clone()
        }
    }

    impl Clone for MemoryTransport {
        fn clone(&self) -> Self {
            Self {
                peer: self.peer.clone(),
                inner: Arc::clone(&self.inner),
            }
        }
    }

    impl Transport for MemoryTransport {
        type Err = MemoryTransportError;
        type MessageStream = crate::sync::stream::BroadcastStream<TransportMessage>;

        fn configure(
            &mut self,
            peer_id: PeerId,
            environment: String,
            _directory_rx: directory::EventStream,
            _runtime: Arc<Runtime>,
        ) -> Result<(), Self::Err> {
            let mut inner = self.inner.lock().unwrap();
            inner.peer_id = Some(peer_id);
            inner.environment = Some(environment);
            Ok(())
        }

        fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
            let inner = self.inner.lock().unwrap();
            match inner.rcv_tx.as_ref() {
                Some(rcv_tx) => Ok(rcv_tx.subscribe().into()),
                None => Err(MemoryTransportError::InvalidOperation),
            }
        }

        fn start(&mut self) -> Result<(), Self::Err> {
            let mut inner = self.inner.lock().unwrap();
            let (rcv_tx, _) = broadcast::channel(128);
            inner.started = true;
            inner.rcv_tx = Some(rcv_tx);
            Ok(())
        }

        fn stop(&mut self) -> Result<(), Self::Err> {
            Ok(())
        }

        fn peer_id(&self) -> Result<&PeerId, Self::Err> {
            unimplemented!()
        }

        fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err> {
            Ok(Cow::Owned("tcp://localhost:5050".to_string()))
        }

        fn send(
            &mut self,
            peers: impl Iterator<Item = Peer>,
            message: TransportMessage,
            _context: SendContext,
        ) -> Result<(), Self::Err> {
            let peers: Vec<_> = peers.collect();

            let mut inner = self.inner.lock().unwrap();
            let environment = inner.environment.clone().unwrap();

            // TODO(oktal): drain_filter
            for peer in &peers {
                let mut i = 0;
                while i < inner.rx_queue.len() {
                    let entry = &inner.rx_queue[i];
                    if (entry.0)(&message, peer) {
                        let entry = inner.rx_queue.remove(i);
                        let response =
                            (entry.1)(message.clone(), self.peer.clone(), environment.clone());
                        let tx = inner.rcv_tx.as_ref().unwrap();
                        tx.send(response).unwrap();
                    } else {
                        i += 1;
                    }
                }
            }

            inner.tx_queue.push((message, peers));
            Ok(())
        }
    }

    macro_rules! impl_handler {
        ($msg:ty, $ty: ty) => {
            impl Handler<$msg> for $ty {
                type Response = ();

                fn handle(&mut self, message: $msg) -> Self::Response {
                    self.add_handled(message);
                }
            }
        };
    }

    impl_handler!(PeerStarted, MemoryDirectoryHandler);
    impl_handler!(PeerStopped, MemoryDirectoryHandler);
    impl_handler!(PeerDecommissioned, MemoryDirectoryHandler);
    impl_handler!(PeerNotResponding, MemoryDirectoryHandler);
    impl_handler!(PeerResponding, MemoryDirectoryHandler);
    impl_handler!(PeerSubscriptionsForTypeUpdated, MemoryDirectoryHandler);
    impl_handler!(PingPeerCommand, MemoryDirectoryHandler);
    impl_handler!(RegisterPeerResponse, MemoryDirectoryHandler);

    impl DirectoryReader for MemoryDirectory {
        fn get(&self, _peer_id: &PeerId) -> Option<Peer> {
            None
        }

        fn get_peers_handling<M: crate::Message>(&self, _message: &M) -> Vec<Peer> {
            let inner = self.inner.lock().unwrap();
            if let Some(peers) = inner.peers.get(M::name()) {
                peers.clone()
            } else {
                vec![]
            }
        }
    }

    impl Directory for MemoryDirectory {
        type EventStream = crate::sync::stream::BroadcastStream<PeerEvent>;
        type Handler = MemoryDirectoryHandler;

        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(Mutex::new(MemoryDirectoryInner::new())),
            })
        }
        fn subscribe(&self) -> Self::EventStream {
            let inner = self.inner.lock().unwrap();
            inner.subscribe().into()
        }

        fn handler(&self) -> Box<Self::Handler> {
            Box::new(MemoryDirectoryHandler {
                inner: Arc::clone(&self.inner),
            })
        }
    }

    struct Fixture {
        rt: Arc<Runtime>,
        peer: Peer,
        environment: String,
        transport: MemoryTransport,
        directory: Arc<MemoryDirectory>,
        bus: Bus<MemoryTransport, MemoryDirectory>,
    }

    impl Fixture {
        fn new_dispatch(
            configuration: BusConfiguration,
            dispatch_fn: impl FnOnce(&mut MessageDispatcher),
        ) -> Self {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();

            let rt = Arc::new(runtime);

            let peer = Peer::test();
            let transport = MemoryTransport::new(peer.clone());

            let environment = "Test".to_string();
            let directory = MemoryDirectory::new();

            let mut dispatcher = MessageDispatcher::new();
            dispatch_fn(&mut dispatcher);
            let bus = Bus::new(
                Arc::clone(&rt),
                configuration,
                transport.clone(),
                directory.clone(),
                dispatcher,
            );

            Self {
                rt,
                peer,
                environment,
                transport,
                directory,
                bus,
            }
        }

        fn new(configuration: BusConfiguration) -> Self {
            Self::new_dispatch(configuration, |_| {})
        }

        fn new_default() -> Self {
            Self::new(Self::configuration())
        }

        fn configuration() -> BusConfiguration {
            BusConfiguration::default()
                .with_directory_endpoints(["tcp://localhost:12500"])
                .with_random_directory(false)
        }

        fn configure(&mut self) -> Result<(), Error> {
            self.bus
                .configure(self.peer.id.clone(), self.environment.clone())
        }

        fn start(&mut self) -> Result<(), Error> {
            self.bus.start()
        }

        fn start_with_registration(&mut self) -> Result<(), Error> {
            // Queue a RegisterPeerResponse to be sent when receiving a RegisterPeerCommand
            self.transport.queue_message(
                |m, _peer| m.is::<RegisterPeerCommand>(),
                |message| {
                    use prost::Message;

                    let response = RegisterPeerResponse { peers: Vec::new() };
                    let message_type = MessageTypeId::of::<RegisterPeerResponse>();

                    MessageExecutionCompleted {
                        command_id: message.id,
                        error_code: 0,
                        payload_type_id: Some(message_type.into_protobuf()),
                        payload: Some(response.encode_to_vec()),
                        response_message: None,
                    }
                },
            );

            // Start the bus
            self.bus.start()
        }
    }

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test", transient)]
    struct BrewCoffeeCommand {
        #[prost(fixed64, tag = 1)]
        id: u64,
    }

    #[derive(Handler)]
    struct BrewCoffeeCommandHandler {
        tx: std::sync::mpsc::Sender<BrewCoffeeCommand>,
    }

    #[derive(Handler)]
    struct BrokenBrewCoffeeCommandHandler {
        tx: std::sync::mpsc::Sender<BrewCoffeeCommand>,
    }

    const BOILER_TOO_COLD: (i32, &'static str) = (0xBADBAD, "Boiler is too cold to brew coffee");

    impl BrokenBrewCoffeeCommandHandler {
        fn new() -> (Box<Self>, std::sync::mpsc::Receiver<BrewCoffeeCommand>) {
            let (tx, rx) = std::sync::mpsc::channel();
            (Box::new(Self { tx }), rx)
        }
    }

    impl BrewCoffeeCommandHandler {
        fn new() -> (Box<Self>, std::sync::mpsc::Receiver<BrewCoffeeCommand>) {
            let (tx, rx) = std::sync::mpsc::channel();
            (Box::new(Self { tx }), rx)
        }
    }

    impl Handler<BrewCoffeeCommand> for BrewCoffeeCommandHandler {
        type Response = ();

        fn handle(&mut self, cmd: BrewCoffeeCommand) -> Self::Response {
            self.tx.send(cmd).unwrap();
        }
    }

    impl Handler<BrewCoffeeCommand> for BrokenBrewCoffeeCommandHandler {
        type Response = Result<(), (i32, &'static str)>;

        fn handle(&mut self, cmd: BrewCoffeeCommand) -> Self::Response {
            self.tx.send(cmd).unwrap();
            Err(BOILER_TOO_COLD)
        }
    }

    #[test]
    fn start_when_bus_is_not_configured_returns_error() {
        let mut fixture = Fixture::new_default();

        // Attempt to start bus without configuring it first
        let res = fixture.bus.start();

        // Assert that start failed with InvalidOperation
        assert!(matches!(res, Err(Error::InvalidOperation)));
    }

    #[test]
    fn configure_bus_will_configure_transport() {
        let mut fixture = Fixture::new_default();

        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(
            fixture.transport.get_peer_id(),
            Some(fixture.peer.id.clone())
        );
        assert_eq!(
            fixture.transport.get_environment(),
            Some(fixture.environment.clone())
        );
    }

    #[test]
    fn send_command_when_bus_is_not_started_returns_error() {
        let mut fixture = Fixture::new_default();

        // Configure bus first
        assert_eq!(fixture.configure().is_ok(), true);

        // Attempt to send a command without starting the bus
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Assert that send failed with InvalidOperation
        assert!(matches!(res, Err(Error::InvalidOperation)));
    }

    fn test_registration_timeout<T: Into<String>>(
        directory_endpoints: impl IntoIterator<Item = T>,
        timeout: Duration,
    ) {
        // Create configuration
        let configuration = BusConfiguration::default()
            .with_directory_endpoints(directory_endpoints)
            .with_registration_timeout(timeout)
            .with_random_directory(false);

        let directory_count = configuration.directory_endpoints.len();

        // Create fixture
        let mut fixture = Fixture::new(configuration);

        // Configure the bus
        fixture.configure().expect("Failed to configure bus");

        // Start the bus
        let res = fixture.bus.start();

        // Make sure a RegisterPeerCommand was sent to every directory
        assert_eq!(
            fixture.transport.get::<RegisterPeerCommand>().len(),
            directory_count
        );

        // Make sure start returned error
        assert_eq!(res.is_err(), true);

        // Make sure registration errored with timeout
        if let Err(Error::Registration(err)) = res {
            assert_eq!(
                err.find(|e| matches!(e, &directory::RegistrationError::Timeout(_)))
                    .is_some(),
                true
            );
        } else {
            assert!(false, "Registration failed with unexpected error {res:?}");
        }
    }

    #[test]
    fn start_bus_will_register_to_directory_timeout() {
        test_registration_timeout(["tcp://localhost:12500"], Duration::from_millis(50));
    }

    #[test]
    fn start_bus_will_register_to_multiple_directories_timeout() {
        test_registration_timeout(
            [
                "tcp://localhost:12500",
                "tcp://localhost:13500",
                "tcp://localhost:14500",
            ],
            Duration::from_millis(50),
        );
    }

    #[test]
    fn start_bus_will_register_to_directory() {
        let mut fixture = Fixture::new_default();

        // Queue a RegisterPeerResponse to be sent when receiving a RegisterPeerCommand
        fixture.transport.queue_message(
            |m, _peer| m.is::<RegisterPeerCommand>(),
            |message| {
                use prost::Message;

                let response = RegisterPeerResponse { peers: Vec::new() };
                let message_type = MessageTypeId::of::<RegisterPeerResponse>();

                MessageExecutionCompleted {
                    command_id: message.id,
                    error_code: 0,
                    payload_type_id: Some(message_type.into_protobuf()),
                    payload: Some(response.encode_to_vec()),
                    response_message: None,
                }
            },
        );

        // Basic assertions
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start().is_ok(), true);
        assert_eq!(fixture.transport.is_started(), true);

        // Make sure a RegisterPeerCommand was sent
        assert_eq!(fixture.transport.get::<RegisterPeerCommand>().len(), 1);

        // Make sure a RegisterPeerResponse was sent and forwarded to the directory
        assert_eq!(
            fixture
                .directory
                .get_handled::<RegisterPeerResponse>()
                .len(),
            1
        );
    }

    #[test]
    fn start_bus_will_try_register_to_multiple_directories() {
        let directory_1 = "tcp://localhost:12500";
        let directory_2 = "tcp://localhost:13500";
        let directory_3 = "tcp://localhost:14500";

        // Create configuration
        let configuration = BusConfiguration::default()
            .with_directory_endpoints([directory_1, directory_2, directory_3])
            .with_registration_timeout(Duration::from_millis(50))
            .with_random_directory(false);

        let mut fixture = Fixture::new(configuration);

        // Queue a RegisterPeerResponse to be sent when receiving a RegisterPeerCommand for last
        // directory
        // Other registrations attempt will timeout
        fixture.transport.queue_message(
            |m, peer| m.is::<RegisterPeerCommand>() && peer.endpoint == *directory_3,
            |message| {
                use prost::Message;

                let response = RegisterPeerResponse { peers: Vec::new() };
                let message_type = MessageTypeId::of::<RegisterPeerResponse>();

                MessageExecutionCompleted {
                    command_id: message.id,
                    error_code: 0,
                    payload_type_id: Some(message_type.into_protobuf()),
                    payload: Some(response.encode_to_vec()),
                    response_message: None,
                }
            },
        );

        // Basic assertions
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start().is_ok(), true);
        assert_eq!(fixture.transport.is_started(), true);

        // Make sure a RegisterPeerCommand was sent for every directory
        assert_eq!(fixture.transport.get::<RegisterPeerCommand>().len(), 3);

        // Make sure a RegisterPeerResponse was sent and forwarded to the directory
        assert_eq!(
            fixture
                .directory
                .get_handled::<RegisterPeerResponse>()
                .len(),
            1
        );
    }

    #[test]
    fn send_command_returns_error_if_no_candidate_peer() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Attempt to send a command with empty directory
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Assert that send failed with NoPeer
        assert!(matches!(res, Err(Error::Send(SendError::NoPeer))));
    }

    #[test]
    fn send_command_returns_error_if_multiple_peers_handle_command() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Setup directory with multiple peers for the same command
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());

        // Attempt to send a command to multiple peers
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Assert that send failed with MultiplePeers
        assert!(matches!(res, Err(Error::Send(SendError::MultiplePeers(_)))));
    }

    #[test]
    fn send_command_returns_error_if_transient_and_peer_not_responding() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Setup directory with multiple peers for the same command
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test().set_not_responding());

        // Attempt to send a command to multiple peers
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Assert that send failed with PeerNotResponding
        assert!(matches!(
            res,
            Err(Error::Send(SendError::PeerNotResponding(_)))
        ));
    }

    #[test]
    fn send_command_will_dispatch_locally_if_multiple_peers_handle_command_with_self() {
        // Create fixture with dispatch
        let configuration = Fixture::configuration();

        let (handler, rx) = BrewCoffeeCommandHandler::new();

        let mut fixture = Fixture::new_dispatch(configuration, |dispatcher| {
            dispatcher
                .add(registry::for_handler(handler, |handler| {
                    handler.handles::<BrewCoffeeCommand>();
                }))
                .unwrap();
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Setup directory with self and other peer
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());

        // Send command
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Assert that command was successfully sent
        assert_eq!(res.is_ok(), true);

        // Assert that command has been locally dispatched
        let result = res.unwrap();
        let result = fixture.rt.block_on(async move { result.await });
        assert!(matches!(result, Ok(None)));

        // Make sure the handler was called
        assert_eq!(rx.try_recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }

    #[test]
    fn dispatch_command_locally_with_error_response() {
        // Create fixture with dispatch
        let configuration = Fixture::configuration();

        let (handler, rx) = BrokenBrewCoffeeCommandHandler::new();

        let mut fixture = Fixture::new_dispatch(configuration, |dispatcher| {
            dispatcher
                .add(registry::for_handler(handler, |handler| {
                    handler.handles::<BrewCoffeeCommand>();
                }))
                .unwrap();
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Setup directory with self
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());

        // Send command
        let res = fixture.bus.send(BrewCoffeeCommand { id: 0xBADC0FFEE });

        // Make sure command was successfully sent
        assert_eq!(res.is_ok(), true);

        // Make sure command resulted in an error
        let result = res.unwrap();
        let result = fixture.rt.block_on(async move { result.await });

        // Make sure command failed with the right error
        if let Err(CommandError::Command { code, message }) = result {
            assert_eq!(code, BOILER_TOO_COLD.0);
            assert_eq!(message, Some(BOILER_TOO_COLD.1.to_string()));
        } else {
            assert!(false, "Command failed with unexpected error {result:?}");
        }

        // Make sure the handler was called
        assert_eq!(rx.try_recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }

    #[test]
    fn dispatch_received_transport_message() {
        // Create fixture with dispatch
        let configuration = Fixture::configuration();

        let (handler, rx) = BrewCoffeeCommandHandler::new();

        let mut fixture = Fixture::new_dispatch(configuration, |dispatcher| {
            dispatcher
                .add(registry::for_handler(handler, |handler| {
                    handler.handles::<BrewCoffeeCommand>();
                }))
                .unwrap();
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Simulate reception of a message
        let sender = Peer::test();
        assert_eq!(
            fixture
                .transport
                .message_received(
                    BrewCoffeeCommand { id: 0xBADC0FFEE },
                    &sender,
                    fixture.environment.clone()
                )
                .is_some(),
            true
        );

        // Make sure the handler was called
        assert_eq!(rx.recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }

    #[test]
    fn dispatch_received_transport_message_locally() {
        // Create fixture with dispatch
        let configuration = Fixture::configuration();

        let (handler, rx) = BrewCoffeeCommandHandler::new();

        let mut fixture = Fixture::new_dispatch(configuration, |dispatcher| {
            dispatcher
                .add(registry::for_handler(handler, |handler| {
                    handler.handles::<BrewCoffeeCommand>();
                }))
                .unwrap();
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().is_ok(), true);

        // Setup directory with self and other peer
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());

        // Simulate reception of a message
        let sender = Peer::test();
        assert_eq!(
            fixture
                .transport
                .message_received(
                    BrewCoffeeCommand { id: 0xBADC0FFEE },
                    &sender,
                    fixture.environment.clone()
                )
                .is_some(),
            true
        );

        // Make sure the handler was called
        assert_eq!(rx.recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }
}
