use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use dyn_clone::clone_box;
use itertools::Itertools;
use tokio::runtime::Handle;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use crate::{
    bus::{CommandFuture, CommandResult, Error, RegistrationError, Result, SendError},
    bus_configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    core::{MessagePayload, SubscriptionMode},
    directory::{
        self, commands::PingPeerCommand, events::PeerSubscriptionsForTypeUpdated, Directory,
        PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
        Registration,
    },
    dispatch::{
        self, DispatchContext, DispatchOutput, DispatchRequest, Dispatched, Dispatcher,
        MessageDispatcher,
    },
    proto::FromProtobuf,
    transport::{MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    BindingKey, BusConfiguration, Command, Event, Message, MessageExt, MessageId, Peer, PeerId,
    Subscription,
};

struct Core<T: Transport, D: Directory> {
    _configuration: BusConfiguration,
    self_peer: Peer,
    environment: String,

    directory: Arc<D>,

    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    snd_tx: mpsc::Sender<SendEntry>,
    dispatch_tx: mpsc::Sender<LocalDispatchRequest>,
    _transport: PhantomData<T>,
}

enum State<T: Transport, D: Directory> {
    Init {
        configuration: BusConfiguration,
        transport: T,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    },

    Configured {
        configuration: BusConfiguration,
        transport: T,
        dispatcher: MessageDispatcher,

        directory: Arc<D>,
        peer_id: PeerId,
        environment: String,
    },

    Starting {},

    Started {
        core: Arc<Core<T, D>>,
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

    async fn send(
        message: &dyn Message,
        snd_tx: &mpsc::Sender<SendEntry>,
        self_peer: &Peer,
        environment: String,
        pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
        peers: impl IntoIterator<Item = Peer>,
    ) -> Result<CommandFuture> {
        // Create the reception channel for the result of the command
        let (tx, rx) = oneshot::channel();

        // Create the `TransportMessage`
        let (id, message) = TransportMessage::create(&self_peer, environment.clone(), message);

        // Insert the command in the collection of pending commands
        pending_commands.lock().unwrap().entry(id).or_insert(tx);

        // Enqueue the message
        snd_tx
            .send(SendEntry {
                message,
                peers: peers.into_iter().collect(),
            })
            .await
            .map_err(|_| Error::Send(SendError::Closed))?;
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
        message: Arc<dyn Message>,
    },

    /// An [`Event`] to be dispatched locally
    Event(Arc<dyn Message>),
}

impl LocalDispatchRequest {
    /// Create a [`LocalDispatchRequest`] for a [`Command`] message
    fn for_command(command: &dyn Command) -> (Self, oneshot::Receiver<CommandResult>) {
        let (tx, rx) = oneshot::channel();
        let message = Arc::from(clone_box(command.up()));

        let request = Self::Command { tx, message };

        (request, rx)
    }

    /// Create a [`LocalDispatchRequest`] for a [`Event`] message
    fn for_event(event: &dyn Event) -> Self {
        let message = Arc::from(clone_box(event.up()));
        Self::Event(message)
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

    async fn dispatch(&mut self, request: DispatchRequest, bus: Arc<dyn crate::Bus>) -> Dispatched {
        self.dispatcher
            .dispatch(DispatchContext::new(request, bus))
            .await
    }

    async fn send(&mut self, message: &dyn Message) {
        let dst_peers = self.directory.get_peers_handling(message);
        if !dst_peers.is_empty() {
            self.send_to(message, dst_peers).await;
        }
    }

    async fn send_to(&mut self, message: &dyn Message, peers: Vec<Peer>) {
        let (_id, message) =
            TransportMessage::create(&self.self_peer, self.environment.clone(), message);

        let _ = self.tx.send(SendEntry { message, peers }).await;
    }
}

/// Reception loop for [`TransportMesssage`] messages
async fn receiver<T: Transport, D: Directory>(
    mut ctx: ReceiverContext<T, D>,
    bus: Arc<dyn crate::Bus>,
) {
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
                    let dispatched = ctx.dispatch(DispatchRequest::Remote(message), bus.clone()).await;

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
                    LocalDispatchRequest::Command { tx, message } => {
                        // Dispatch command locally
                        let dispatched = ctx.dispatch(DispatchRequest::Local(message), bus.clone()).await;

                        // Retrieve the `CommandResult`
                        let command_result: Option<CommandResult> = dispatched.try_into().ok();

                        // Resolve command future
                        if let Some(command_result) = command_result {
                            let _ = tx.send(command_result);
                        }
                    },
                    LocalDispatchRequest::Event(message) => {
                        // Dispatch event locally
                        let _dispatched = ctx.dispatch(DispatchRequest::Local(message), bus.clone()).await;
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
    subscriptions: Vec<Subscription>,
    environment: &String,
    configuration: &BusConfiguration,
) -> Result<Registration> {
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
            subscriptions.clone(),
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

/// Load the list of subscriptions to send at registration from a [`MessageDispatcher`]
fn get_startup_subscriptions(dispatcher: &MessageDispatcher) -> Result<Vec<Subscription>> {
    let handled_messages = dispatcher.handled_mesages().map_err(Error::Dispatch)?;

    let mut subscriptions = vec![];
    for (message_type, subscription_mode, bindings) in handled_messages {
        // If we are in `Auto` subscription mode, add the subscription to the list of startup
        // subscriptions
        if subscription_mode == SubscriptionMode::Auto {
            // If we have bindings for the subscription, create a subscription for every binding
            if bindings.len() > 0 {
                subscriptions.extend(
                    bindings
                        .iter()
                        .map(|b| Subscription::new(message_type.clone(), b.clone())),
                );
            } else {
                // Create a subscription with an empty default (*) binding
                subscriptions.push(Subscription::new(
                    message_type.clone(),
                    BindingKey::default(),
                ));
            }
        }
    }

    Ok(subscriptions)
}

impl<T: Transport, D: Directory> Core<T, D> {
    async fn send(&self, command: &dyn Command) -> Result<CommandFuture> {
        // Retrieve the list of peers handling the command from the directory
        let peers = self.directory.get_peers_handling(command.up());

        let self_dst_peer = peers.iter().find(|&p| p.id == self.self_peer.id);
        // TODO(oktal): add local dispatch toggling
        // If we are the receiver of the command, do a local dispatch
        if self_dst_peer.is_some() {
            // Create the local dispatch request for the command
            let (request, rx) = LocalDispatchRequest::for_command(command);

            // Send the command
            self.dispatch_tx
                .send(request)
                .await
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
            if !dst_peer.is_responding && command.is_transient() && !command.is_infrastructure() {
                Err(Error::Send(SendError::PeerNotResponding(dst_peer)))
            } else {
                // Send the command
                SenderContext::<T>::send(
                    command.up(),
                    &self.snd_tx,
                    &self.self_peer,
                    self.environment.clone(),
                    self.pending_commands.clone(),
                    std::iter::once(dst_peer),
                )
                .await
            }
        }
    }

    async fn send_to(&self, command: &dyn Command, peer: crate::Peer) -> Result<CommandFuture> {
        // Attempting to send a non-persistent command to a non-responding peer result in
        // an error
        if !peer.is_responding && command.is_transient() && !command.is_infrastructure() {
            return Err(Error::Send(SendError::PeerNotResponding(peer)));
        }

        // Send the command
        SenderContext::<T>::send(
            command.up(),
            &self.snd_tx,
            &self.self_peer,
            self.environment.clone(),
            self.pending_commands.clone(),
            std::iter::once(peer),
        )
        .await
    }
}

#[async_trait]
impl<T: Transport, D: Directory> crate::Bus for Core<T, D> {
    fn configure(&self, _peer_id: PeerId, _environment: String) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn start(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn stop(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn send(&self, command: &dyn Command) -> Result<CommandFuture> {
        self.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> Result<CommandFuture> {
        self.send_to(command, peer).await
    }
}

struct Bus<T: Transport, D: Directory> {
    inner: Mutex<Option<State<T, D>>>,
}

impl<T: Transport, D: Directory> Bus<T, D> {
    fn new(
        configuration: BusConfiguration,
        transport: T,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    ) -> Self {
        Self {
            inner: Mutex::new(Some(State::Init {
                configuration,
                transport,
                directory,
                dispatcher,
            })),
        }
    }
}

impl<T: Transport, D: Directory> Bus<T, D> {
    fn configure(&self, peer_id: PeerId, environment: String) -> Result<()> {
        let mut state = self.inner.lock().unwrap();

        let (inner, res) = match state.take() {
            Some(State::Init {
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
                    .configure(peer_id.clone(), environment.clone(), directory_rx)
                    .map_err(|e| Error::Transport(e.into()))?;

                (
                    Some(State::Configured {
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

        *state = inner;
        res
    }

    async fn start(&self) -> Result<()> {
        let state = {
            let mut state_lock = self.inner.lock().unwrap();
            let state = state_lock.take();
            *state_lock = Some(State::Starting {}); // TODO restore valid state on error
            state
        };

        if let Some(State::Configured {
            configuration,
            mut transport,
            mut dispatcher,
            peer_id,
            directory,
            environment,
        }) = state
        {
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

            // Setup peer directory client handler
            let directory_handler = directory.handler();
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

            // Retrieve the list of subscriptions that should be sent at startup
            let startup_subscriptions = get_startup_subscriptions(&dispatcher)?;

            // Register to directory
            let registration = register(
                &mut transport,
                &self_peer,
                startup_subscriptions,
                &environment,
                &configuration,
            )
            .await?;

            // Handle peer directory response
            if let Ok(response) = registration.result {
                directory.handle_registration(response);
            }

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

            // Create Core
            let core = Arc::new(Core::<T, D> {
                _configuration: configuration,
                self_peer,
                environment,
                directory,
                pending_commands,
                snd_tx,
                dispatch_tx,
                _transport: PhantomData,
            });

            // Start sender
            let tx_handle = Handle::current().spawn(sender(snd_ctx));

            // Start receiver
            let rx_handle = Handle::current().spawn(receiver(rcv_ctx, core.clone()));

            // Transition to started state
            let mut state_lock = self.inner.lock().unwrap();
            *state_lock = Some(State::Started {
                core,
                rx_handle,
                tx_handle,
            });

            Ok(())
        } else {
            let mut state_lock = self.inner.lock().unwrap();
            *state_lock = state;
            Err(Error::InvalidOperation)
        }
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    async fn send(&self, command: &dyn Command) -> Result<CommandFuture> {
        let core = self.core()?;
        core.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: crate::Peer) -> Result<CommandFuture> {
        let core = self.core()?;
        core.send_to(command, peer).await
    }

    fn core(&self) -> Result<Arc<Core<T, D>>> {
        let inner = self.inner.lock().unwrap();
        match inner.as_ref() {
            Some(State::Started { core, .. }) => Ok(core.clone()),
            _ => Err(Error::InvalidOperation),
        }
    }
}

#[async_trait]
impl<T: Transport, D: Directory> crate::Bus for Bus<T, D> {
    fn configure(&self, peer_id: PeerId, environment: String) -> Result<()> {
        self.configure(peer_id, environment)
    }

    async fn start(&self) -> Result<()> {
        self.start().await
    }

    async fn stop(&self) -> Result<()> {
        self.stop().await
    }

    async fn send(&self, command: &dyn Command) -> Result<CommandFuture> {
        self.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> Result<CommandFuture> {
        self.send_to(command, peer).await
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
    dispatcher: MessageDispatcher,
}

impl<T: Transport> BusBuilder<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            peer_id: None,
            configuration: None,
            environment: None,
            dispatcher: MessageDispatcher::new(),
        }
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

    pub fn create(self) -> std::result::Result<impl crate::Bus, CreateError<Error>> {
        let (transport, peer_id, configuration, environment) = (
            self.transport,
            self.peer_id.unwrap_or(Self::testing_peer_id()),
            self.configuration
                .ok_or(CreateError::MissingConfiguration)?,
            self.environment.ok_or(CreateError::MissingConfiguration)?,
        );

        let dispatcher = self.dispatcher;

        // Create peer directory client
        let client = directory::Client::new();

        // Create the bus
        let bus = Bus::new(configuration, transport, client, dispatcher);

        // Configure the bus
        bus.configure(peer_id, environment)
            .map_err(CreateError::Configure)?;
        Ok(bus)
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
        subscribe, Handler, MessageDescriptor,
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

        /// Ad a `message` to the list of handled messages by the directory
        fn add_handled<M: MessageDescriptor + Send + Sync + 'static>(&mut self, message: M) {
            let entry = self.messages.entry(M::name()).or_insert(Vec::new());
            entry.push(Arc::new(message));
        }

        fn add_peer_for<M: MessageDescriptor>(&mut self, peer: Peer) {
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
        fn get_handled<M: MessageDescriptor + Send + Sync + 'static>(&self) -> Vec<Arc<M>> {
            let inner = self.inner.lock().unwrap();

            match inner.messages.get(M::name()) {
                Some(entry) => entry
                    .iter()
                    .filter_map(|m| m.clone().downcast::<M>().ok())
                    .collect(),
                None => vec![],
            }
        }

        /// Add a peer that should hande the `Message`
        fn add_peer_for<M: MessageDescriptor>(&self, peer: Peer) {
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
        fn add_handled<M: MessageDescriptor + Send + Sync + 'static>(&mut self, message: M) {
            let mut inner = self.inner.lock().unwrap();
            inner.add_handled(message);
        }
    }

    #[derive(Debug, thiserror::Error)]
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
        fn queue_message<M: Message>(
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
        fn get<M: MessageDescriptor + prost::Message + Default>(&self) -> Vec<(M, Vec<Peer>)> {
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
        ) -> std::result::Result<(), Self::Err> {
            let mut inner = self.inner.lock().unwrap();
            inner.peer_id = Some(peer_id);
            inner.environment = Some(environment);
            Ok(())
        }

        fn subscribe(&self) -> std::result::Result<Self::MessageStream, Self::Err> {
            let inner = self.inner.lock().unwrap();
            match inner.rcv_tx.as_ref() {
                Some(rcv_tx) => Ok(rcv_tx.subscribe().into()),
                None => Err(MemoryTransportError::InvalidOperation),
            }
        }

        fn start(&mut self) -> std::result::Result<(), Self::Err> {
            let mut inner = self.inner.lock().unwrap();
            let (rcv_tx, _) = broadcast::channel(128);
            inner.started = true;
            inner.rcv_tx = Some(rcv_tx);
            Ok(())
        }

        fn stop(&mut self) -> std::result::Result<(), Self::Err> {
            Ok(())
        }

        fn peer_id(&self) -> std::result::Result<&PeerId, Self::Err> {
            unimplemented!()
        }

        fn inbound_endpoint(&self) -> std::result::Result<Cow<'_, str>, Self::Err> {
            Ok(Cow::Owned("tcp://localhost:5050".to_string()))
        }

        fn send(
            &mut self,
            peers: impl Iterator<Item = Peer>,
            message: TransportMessage,
            _context: SendContext,
        ) -> std::result::Result<(), Self::Err> {
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
            #[subscribe(auto)]
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

    impl DirectoryReader for MemoryDirectory {
        fn get(&self, _peer_id: &PeerId) -> Option<Peer> {
            None
        }

        fn get_peers_handling(&self, message: &dyn Message) -> Vec<Peer> {
            let inner = self.inner.lock().unwrap();
            if let Some(peers) = inner.peers.get(message.name()) {
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

        fn handle_registration(&self, response: RegisterPeerResponse) {
            let mut inner = self.inner.lock().unwrap();
            inner.add_handled(response);
        }

        fn handler(&self) -> Box<Self::Handler> {
            Box::new(MemoryDirectoryHandler {
                inner: Arc::clone(&self.inner),
            })
        }
    }

    struct Fixture {
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
            let peer = Peer::test();
            let transport = MemoryTransport::new(peer.clone());

            let environment = "Test".to_string();
            let directory = MemoryDirectory::new();

            let mut dispatcher = MessageDispatcher::new();
            dispatch_fn(&mut dispatcher);
            let bus = Bus::new(
                configuration,
                transport.clone(),
                directory.clone(),
                dispatcher,
            );

            Self {
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

        fn configure(&mut self) -> std::result::Result<(), Error> {
            self.bus
                .configure(self.peer.id.clone(), self.environment.clone())
        }

        async fn start(&mut self) -> std::result::Result<(), Error> {
            self.bus.start().await
        }

        async fn start_with_registration(&mut self) -> std::result::Result<(), Error> {
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
            self.bus.start().await
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

    #[subscribe(auto)]
    impl Handler<BrewCoffeeCommand> for BrewCoffeeCommandHandler {
        type Response = ();

        fn handle(&mut self, cmd: BrewCoffeeCommand) -> Self::Response {
            self.tx.send(cmd).unwrap();
        }
    }

    #[subscribe(manual)]
    impl Handler<BrewCoffeeCommand> for BrokenBrewCoffeeCommandHandler {
        type Response = std::result::Result<(), (i32, &'static str)>;

        fn handle(&mut self, cmd: BrewCoffeeCommand) -> Self::Response {
            self.tx.send(cmd).unwrap();
            Err(BOILER_TOO_COLD)
        }
    }

    #[tokio::test]
    async fn start_when_bus_is_not_configured_returns_error() {
        let fixture = Fixture::new_default();

        // Attempt to start bus without configuring it first
        let res = fixture.bus.start().await;

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

    #[tokio::test]
    async fn send_command_when_bus_is_not_started_returns_error() {
        let mut fixture = Fixture::new_default();

        // Configure bus first
        assert_eq!(fixture.configure().is_ok(), true);

        // Attempt to send a command without starting the bus
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with InvalidOperation
        assert!(matches!(res, Err(Error::InvalidOperation)));
    }

    async fn test_registration_timeout<T: Into<String>>(
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
        let res = fixture.bus.start().await;

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

    #[tokio::test]
    async fn start_bus_will_register_to_directory_timeout() {
        test_registration_timeout(["tcp://localhost:12500"], Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn start_bus_will_register_to_multiple_directories_timeout() {
        test_registration_timeout(
            [
                "tcp://localhost:12500",
                "tcp://localhost:13500",
                "tcp://localhost:14500",
            ],
            Duration::from_millis(50),
        )
        .await;
    }

    #[tokio::test]
    async fn start_bus_will_register_to_directory() {
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
        assert_eq!(fixture.start().await.is_ok(), true);
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

    #[tokio::test]
    async fn start_bus_will_register_and_subscribe_automatically_to_directory() {
        let (handler, _) = BrewCoffeeCommandHandler::new();

        // Create fixture with dispatch
        let mut fixture = Fixture::new_dispatch(Fixture::configuration(), |dispatcher| {
            dispatcher
                .add(registry::for_handler(handler, |handler| {
                    handler.handles::<BrewCoffeeCommand>();
                }))
                .unwrap();
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Make sure a RegisterPeerCommand was sent with a subscription to `BrewCoffeeCommand`
        let register_command = fixture.transport.get::<RegisterPeerCommand>();
        let subscriptions = register_command
            .get(0)
            .map(|(cmd, _)| &cmd.peer.subscriptions);
        assert_eq!(subscriptions.is_some(), true);
        let subscriptions = subscriptions.unwrap();

        // Make sure a subscription to `BrewCoffeeCommand` was made
        let subscription = subscriptions
            .iter()
            .find(|s| s.message_type_id.is::<BrewCoffeeCommand>());
        assert_eq!(subscription.is_some(), true);
    }

    #[tokio::test]
    async fn start_bus_will_try_register_to_multiple_directories() {
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
        assert_eq!(fixture.start().await.is_ok(), true);
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

    #[tokio::test]
    async fn send_command_returns_error_if_no_candidate_peer() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Attempt to send a command with empty directory
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with NoPeer
        assert!(matches!(res, Err(Error::Send(SendError::NoPeer))));
    }

    #[tokio::test]
    async fn send_command_returns_error_if_multiple_peers_handle_command() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with multiple peers for the same command
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());

        // Attempt to send a command to multiple peers
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with MultiplePeers
        assert!(matches!(res, Err(Error::Send(SendError::MultiplePeers(_)))));
    }

    #[tokio::test]
    async fn send_command_returns_error_if_transient_and_peer_not_responding() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with multiple peers for the same command
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test().set_not_responding());

        // Attempt to send a command to multiple peers
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with PeerNotResponding
        assert!(matches!(
            res,
            Err(Error::Send(SendError::PeerNotResponding(_)))
        ));
    }

    #[tokio::test]
    async fn send_command_will_dispatch_locally_if_multiple_peers_handle_command_with_self() {
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
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with self and other peer
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());

        // Send command
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that command was successfully sent
        assert_eq!(res.is_ok(), true);

        // Assert that command has been locally dispatched
        let result = res.unwrap().await;
        assert!(matches!(result, Ok(None)));

        // Make sure the handler was called
        assert_eq!(rx.try_recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }

    #[tokio::test]
    async fn dispatch_command_locally_with_error_response() {
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
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with self
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());

        // Send command
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Make sure command was successfully sent
        assert_eq!(res.is_ok(), true);

        // Make sure command resulted in an error
        let result = res.unwrap().await;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_received_transport_message() {
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
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_received_transport_message_locally() {
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
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

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
