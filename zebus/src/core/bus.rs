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

use crate::{
    bus::{CommandFuture, CommandResult},
    bus_configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    core::MessagePayload,
    directory::{
        self, commands::PingPeerCommand, events::PeerSubscriptionsForTypeUpdated,
        PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
        Registration,
    },
    dispatch::{
        self, AnyMessage, DispatchOutput, DispatchRequest, Dispatched, Dispatcher,
        MessageDispatcher,
    },
    proto::FromProtobuf,
    transport::{self, MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    Bus, BusConfiguration, Command, Event, Handler, MessageId, Peer, PeerId,
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
}

#[derive(Debug, Error)]
pub enum SendError {
    /// An attempt to send a [`Command`] resulted in no candidat peer
    #[error("unable to find peer for command")]
    NoPeer,

    /// An attempt to send a [`Command`] resulted in multiple candidate peers
    #[error("can not send a command to multiple peers: {0:?}")]
    MultiplePeers(Vec<Peer>),

    /// The sender has been closed
    #[error("sender has been closed")]
    Closed
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

enum State<T: Transport> {
    Init {
        runtime: Runtime,
        configuration: BusConfiguration,
        transport: T,
        dispatcher: MessageDispatcher,
    },

    Configured {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        transport: T,
        dispatcher: MessageDispatcher,

        directory: directory::Client,
        peer_id: PeerId,
        environment: String,
    },

    Started {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        self_peer: Peer,
        environment: String,

        directory: directory::Client,

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
    }
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
            message: Arc::new(command)
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
            message: Arc::new(event)
        }
    }
}

struct ReceiverContext {
    transport_rx: transport::Receiver,
    dispatch_rx: mpsc::Receiver<LocalDispatchRequest>,
    tx: mpsc::Sender<SendEntry>,
    self_peer: Peer,
    environment: String,
    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    directory: directory::Client,
    dispatcher: MessageDispatcher,
}

impl ReceiverContext {
    fn new(
        transport_rx: transport::Receiver,
        tx: mpsc::Sender<SendEntry>,
        self_peer: Peer,
        environment: String,
        directory: directory::Client,
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
                transport_rx,
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
async fn receiver(mut ctx: ReceiverContext) {
    loop {
        tokio::select! {

            // Handle inbound TransportMessage
            Some(message) = ctx.transport_rx.recv() => {
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

fn try_register<T: Transport>(
    transport: &mut T,
    receiver: &transport::Receiver,
    runtime: Arc<Runtime>,
    self_peer: Peer,
    environment: String,
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
        match directory::registration::block_on(
            Arc::clone(&runtime),
            transport,
            receiver,
            self_peer.clone(),
            environment.clone(),
            directory_peer.clone(),
            timeout,
        ) {
            Ok(r) => return Ok(r),
            Err(e) => error.add(directory_peer.clone(), e),
        }
    }

    Err(Error::Registration(error))
}

struct BusImpl<T: Transport> {
    inner: Option<State<T>>,
}

impl<T: Transport> BusImpl<T> {
    fn new(
        runtime: Runtime,
        configuration: BusConfiguration,
        transport: T,
        dispatcher: MessageDispatcher,
    ) -> Self {
        Self {
            inner: Some(State::Init {
                runtime,
                configuration,
                transport,
                dispatcher,
            }),
        }
    }
}

impl<T: Transport> Bus for BusImpl<T> {
    type Err = Error;

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(State::Init {
                runtime,
                configuration,
                mut transport,
                dispatcher,
            }) => {
                // Wrap tokio's runtime inside an Arc to share it with other components
                let runtime = Arc::new(runtime);

                // Create peer directory client
                let (directory, directory_rx) = directory::Client::new();

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

    fn start(&mut self) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(State::Configured {
                runtime,
                configuration,
                mut transport,
                mut dispatcher,
                peer_id,
                mut directory,
                environment,
            }) => {
                // Start transport
                let transport_receiver =
                    transport.start().map_err(|e| Error::Transport(e.into()))?;
                let endpoint = transport
                    .inbound_endpoint()
                    .map_err(|e| Error::Transport(e.into()))?;

                // Register to directory
                let self_peer = Peer {
                    id: peer_id.clone(),
                    endpoint: endpoint.to_string(),
                    is_up: true,
                    is_responding: true,
                };

                let registration = try_register(
                    &mut transport,
                    &transport_receiver,
                    Arc::clone(&runtime),
                    self_peer.clone(),
                    environment.clone(),
                    &configuration,
                )?;

                if let Ok(response) = registration.result {
                    directory.handle(response);
                }

                // Setup peer directory client handler
                dispatcher
                    .add(dispatch::registry::for_handler(directory.handler(), |h| {
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

                // Create sender
                let (snd_tx, snd_ctx) = SenderContext::new(transport);

                // Create receiver
                let (rcv_ctx, dispatch_tx, pending_commands) = ReceiverContext::new(
                    transport_receiver,
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

    fn stop(&mut self) -> Result<(), Self::Err> {
        todo!()
    }

    fn send<C: Command + prost::Message + Send + 'static>(
        &mut self,
        command: C,
    ) -> Result<CommandFuture, Self::Err> {
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

                // Make sure there is only one peer handling the command
                let dst_peer = peers
                    .into_iter()
                    .at_most_one()
                    .map_err(|e| Error::Send(SendError::MultiplePeers(e.collect())))?
                    .ok_or(Error::Send(SendError::NoPeer))?;

                // TODO(oktal): add local dispatch toggling
                // If we are the receiver of the command, do a local dispatch
                if dst_peer.id == self_peer.id {
                    // Create the local dispatch request for the command
                    let (request, rx)  = LocalDispatchRequest::for_command(command);

                    // Send the command
                    // TODO(oktal): use async send
                    dispatch_tx.blocking_send(request).map_err(|_| Error::Send(SendError::Closed))?;

                    // Return the future
                    Ok(CommandFuture(rx))
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
            _ => Err(Error::InvalidOperation),
        }
    }

    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: C,
        peer: crate::Peer,
    ) -> Result<CommandFuture, Self::Err> {
        match self.inner.as_mut() {
            Some(State::Started {
                ref snd_tx,
                ref pending_commands,
                ref self_peer,
                ref environment,
                ..
            }) => {
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

    pub fn create(self) -> Result<impl Bus, CreateError<Error>> {
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

        // Create the bus
        let mut bus = BusImpl::new(runtime, configuration, transport, dispatcher);

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
