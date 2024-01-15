use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use dyn_clone::clone_box;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower_service::Service;
use tracing::{error, info};

use crate::{
    bus::{CommandFuture, CommandResult, Error, RegistrationError, Result, SendError},
    bus_configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    core::{MessagePayload, SubscriptionMode},
    directory::{self, Directory, Registration},
    dispatch::{
        self, DispatchOutput, DispatchRequest, DispatchService, Dispatched, InvokerService,
        MessageDispatcher,
    },
    proto::FromProtobuf,
    transport::{MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    BindingKey, BusConfiguration, Command, Event, Message, MessageExt, MessageId, Peer, PeerId,
    Subscription,
};

struct Core<T: Transport, D: Directory> {
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

        configuration: BusConfiguration,
        directory: Arc<D>,
        peer_id: PeerId,
        environment: String,

        cancellation: CancellationToken,

        tx_handle: tokio::task::JoinHandle<TxHandle<T>>,
        rx_handle: tokio::task::JoinHandle<RxHandle>,
    },
}

#[derive(Debug)]
enum SendEntry {
    Message {
        message: TransportMessage,
        peers: Vec<Peer>,
    },
    Unregister {
        tx: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

struct TxHandle<T: Transport> {
    transport: T,
}

struct Sender<T: Transport> {
    transport: T,
    configuration: BusConfiguration,
    cancellation: CancellationToken,
    rx: mpsc::Receiver<SendEntry>,
}

impl<T: Transport> Sender<T> {
    fn new(
        transport: T,
        configuration: BusConfiguration,
        cancellation: CancellationToken,
    ) -> (mpsc::Sender<SendEntry>, Self) {
        // TODO(oktal): hardcoded limit
        let (tx, rx) = mpsc::channel(128);
        (
            tx,
            Self {
                transport,
                configuration,
                cancellation,
                rx,
            },
        )
    }

    async fn run(mut self) -> TxHandle<T> {
        loop {
            tokio::select! {
                // We have been cancelled
                _ = self.cancellation.cancelled() => break,

                // We received an entry
                Some(entry) = self.rx.recv() => {
                    if let Err(e) = self.handle_entry(entry).await {
                        error!("{e}");
                    }
                }
            }
        }

        // Yield back the transport
        TxHandle {
            transport: self.transport,
        }
    }

    async fn send(
        message: &dyn Command,
        snd_tx: &mpsc::Sender<SendEntry>,
        self_peer: &Peer,
        environment: &str,
        pending_commands: &Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>,
        peers: impl IntoIterator<Item = Peer>,
    ) -> Result<CommandFuture> {
        // Create the reception channel for the result of the command
        let (tx, rx) = oneshot::channel();

        // Create the `TransportMessage`
        let (id, message) =
            TransportMessage::create(&self_peer, environment.to_string(), message.up());

        // Insert the command in the collection of pending commands
        pending_commands.lock().unwrap().entry(id).or_insert(tx);

        // Enqueue the message
        snd_tx
            .send(SendEntry::Message {
                message,
                peers: peers.into_iter().collect(),
            })
            .await
            .map_err(|_| Error::Send(SendError::Closed))?;
        Ok(CommandFuture(rx))
    }

    async fn publish(
        message: &dyn Event,
        snd_tx: &mpsc::Sender<SendEntry>,
        self_peer: &Peer,
        environment: &str,
        peers: impl IntoIterator<Item = Peer>,
    ) -> Result<()> {
        // Create the `TransportMessage`
        let (_, message) =
            TransportMessage::create(&self_peer, environment.to_string(), message.up());

        // Enqueue the message
        snd_tx
            .send(SendEntry::Message {
                message,
                peers: peers.into_iter().collect(),
            })
            .await
            .map_err(|_| Error::Send(SendError::Closed))?;
        Ok(())
    }

    async fn handle_entry(&mut self, entry: SendEntry) -> Result<()> {
        match entry {
            SendEntry::Message { message, peers } => self
                .transport
                .send(peers.into_iter(), message, SendContext::default())
                .map_err(|e| Error::Transport(e.into())),
            SendEntry::Unregister { tx } => {
                tx.send(unregister(&mut self.transport, &self.configuration).await)
                    .expect("channel closed unexpectdely");
                Ok(())
            }
        }
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
    Event {
        tx: oneshot::Sender<()>,
        message: Arc<dyn Message>,
    },
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
    fn for_event(event: &dyn Event) -> (Self, oneshot::Receiver<()>) {
        // Create a singleshot channel so that the caller can await for the result of
        // the dispatch execution
        let (tx, rx) = oneshot::channel();
        let message = Arc::from(clone_box(event.up()));

        let request = Self::Event { tx, message };
        (request, rx)
    }
}

struct RxHandle {
    dispatcher: MessageDispatcher,
}

struct Receiver<T: Transport, D: Directory> {
    rcv_rx: T::MessageStream,
    dispatch_rx: mpsc::Receiver<LocalDispatchRequest>,
    tx: mpsc::Sender<SendEntry>,
    self_peer: Peer,
    environment: String,
    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    directory: Arc<D>,
    dispatcher: MessageDispatcher,
    cancellation: CancellationToken,
}

impl<T: Transport, D: Directory> Receiver<T, D> {
    fn new(
        transport_rx: T::MessageStream,
        tx: mpsc::Sender<SendEntry>,
        self_peer: Peer,
        environment: String,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
        cancellation: CancellationToken,
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
                cancellation,
            },
            dispatch_tx,
            pending_commands,
        )
    }

    async fn run(mut self, bus: Arc<dyn crate::Bus>) -> RxHandle {
        loop {
            tokio::select! {
                // We have been cancelled
                _ = self.cancellation.cancelled() => break,

                // Handle inbound TransportMessage
                Some(message) = self.rcv_rx.next() => {
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
                            let mut pending_commands = self.pending_commands.lock().unwrap();
                            if let Some(pending_command_tx) = pending_commands.remove(&command_id.value()) {
                                // Resolve the command with the execution result
                                let _ = pending_command_tx.send(message_execution_completed.into());
                            }
                        }
                    }
                    // Handle message
                    else {
                        // Dispatch message
                        match self.dispatch(DispatchRequest::remote(message, bus.clone())).await {
                            Ok(dispatched) => {
                                // Retrieve the output of the dispatch
                                let output: DispatchOutput = dispatched.into();

                                // If the message that has been dispatched is a Command, send back the
                                // MessageExecutionCompleted
                                if let Some(completed) = output.completed {
                                    self.send_to(
                                        &completed,
                                        vec![output.originator.expect("missing originator")],
                                    )
                                    .await;
                                }

                                // Publish [`MessageProcessingFailed`] if some handlers failed
                                if let Some(failed) = output.failed {
                                    self.send(&failed).await;
                                }
                            },
                            Err(e) => {
                                println!("Failed to dispatch: {e}");
                            }
                        }

                    }
                }

                // Handle local message dispatch request
                Some(request) = self.dispatch_rx.recv() => {
                    // TODO(oktal): send `MessageProcessingFailed` if dispatch failed ?
                    match request {
                        LocalDispatchRequest::Command { tx, message } => {
                            // Dispatch command locally
                            let dispatched = self.dispatch(DispatchRequest::local(message, bus.clone())).await.unwrap();

                            // Retrieve the `CommandResult`
                            let command_result: Option<CommandResult> = dispatched.try_into().ok();

                            // Resolve command future
                            if let Some(command_result) = command_result {
                                let _ = tx.send(command_result);
                            }
                        }
                        LocalDispatchRequest::Event { tx, message } => {
                            // Dispatch event locally
                            let _dispatched = self.dispatch(DispatchRequest::local(message, bus.clone())).await.unwrap();

                            // Notify that the event has been dispatched
                            let _ = tx.send(());
                        }
                    }

                }
            }
        }

        // Yield back the dispatcher
        RxHandle {
            dispatcher: self.dispatcher,
        }
    }

    async fn dispatch(
        &mut self,
        request: DispatchRequest,
    ) -> std::result::Result<Dispatched, dispatch::Error> {
        self.dispatcher.call(request).await
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

        let _ = self.tx.send(SendEntry::Message { message, peers }).await;
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
    let directory_peers = configuration.directory_peers();

    let timeout = configuration.registration_timeout;
    let mut error = RegistrationError::new();
    for directory_peer in directory_peers {
        info!("register on directory {directory_peer}",);

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
            Err(e) => {
                error!("failed to register on directory {directory_peer}: {e}");
                error.add(directory_peer.clone(), e)
            }
        }
    }

    Err(Error::Registration(error))
}

async fn unregister<T: Transport>(
    transport: &mut T,
    configuration: &BusConfiguration,
) -> Result<()> {
    let directory_peers = configuration.directory_peers();

    let timeout = configuration.registration_timeout;

    let mut error = RegistrationError::new();

    for directory_peer in directory_peers {
        info!("unregister from directory {directory_peer}",);

        match directory::registration::unregister(transport, directory_peer.clone(), timeout).await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                error!("failed to unregister from directory {directory_peer}: {e}");
                error.add(directory_peer, e);
            }
        }
    }

    Err(Error::Registration(error))
}

/// Load the list of subscriptions to send at registration from a [`MessageDispatcher`]
fn get_startup_subscriptions<D: DispatchService>(
    dispatcher: &D,
) -> std::result::Result<Vec<Subscription>, D::Error> {
    let descriptors = dispatcher.descriptors()?;
    let mut subscriptions = vec![];
    for descriptor in descriptors {
        // If we are in `Auto` subscription mode, add the subscription to the list of startup
        // subscriptions
        if descriptor.subscription_mode == SubscriptionMode::Auto {
            // If we have bindings for the subscription, create a subscription for every binding
            if descriptor.bindings.len() > 0 {
                subscriptions.extend(
                    descriptor
                        .bindings
                        .iter()
                        .map(|b| Subscription::new(descriptor.message.clone(), b.clone())),
                );
            } else {
                // Create a subscription with an empty default (*) binding
                subscriptions.push(Subscription::new(
                    descriptor.message.clone(),
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
                Sender::<T>::send(
                    command,
                    &self.snd_tx,
                    &self.self_peer,
                    &self.environment,
                    &self.pending_commands,
                    std::iter::once(dst_peer),
                )
                .await
            }
        }
    }

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> Result<CommandFuture> {
        // Attempting to send a non-persistent command to a non-responding peer result in
        // an error
        if !peer.is_responding && command.is_transient() && !command.is_infrastructure() {
            return Err(Error::Send(SendError::PeerNotResponding(peer)));
        }

        // Send the command
        Sender::<T>::send(
            command,
            &self.snd_tx,
            &self.self_peer,
            &self.environment,
            &self.pending_commands,
            std::iter::once(peer),
        )
        .await
    }

    async fn publish(&self, event: &dyn Event) -> Result<()> {
        // Retrieve the list of peers handling the event from the directory
        let peers = self.directory.get_peers_handling(event.up());

        let self_dst_peer = peers.iter().find(|&p| p.id == self.self_peer.id);
        // TODO(oktal): add local dispatch toggling
        // If we are a receiver of the event, do a local dispatch
        if self_dst_peer.is_some() {
            // Create the local dispatch request for the command
            let (request, rx) = LocalDispatchRequest::for_event(event);

            // Send the event
            self.dispatch_tx
                .send(request)
                .await
                .map_err(|_| Error::Send(SendError::Closed))?;

            // Wait for the event to be handled prior to sending it over the bus
            rx.await.map_err(|_| Error::Send(SendError::Closed))?;
        }

        let dst_peers = peers.into_iter().filter(|p| p.id != self.self_peer.id);

        // Send the event
        Sender::<T>::publish(
            event,
            &self.snd_tx,
            &self.self_peer,
            &self.environment,
            dst_peers,
        )
        .await?;

        Ok(())
    }

    async fn unregister(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.snd_tx
            .send(SendEntry::Unregister { tx })
            .await
            .expect("channel has been closed unexpectedely");

        rx.await.expect("channel has been closed unexpectedely")
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

    async fn publish(&self, event: &dyn Event) -> Result<()> {
        self.publish(event).await
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
        info!("starting bus...");

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
                .add(Box::new(directory_handler))
                .map_err(Error::Dispatch)?;

            // Retrieve the list of subscriptions that should be sent at startup
            let startup_subscriptions =
                get_startup_subscriptions(&dispatcher).map_err(Error::Dispatch)?;

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

            // Create cancellation token
            let cancellation = CancellationToken::new();

            // Create sender
            let (snd_tx, sender) =
                Sender::new(transport, configuration.clone(), cancellation.clone());

            // Create receiver
            let (receiver, dispatch_tx, pending_commands) = Receiver::<T, D>::new(
                rcv_rx,
                snd_tx.clone(),
                self_peer.clone(),
                environment.clone(),
                directory.clone(),
                dispatcher,
                cancellation.clone(),
            );

            // Create Core
            let core = Arc::new(Core::<T, D> {
                self_peer,
                environment: environment.clone(),
                directory: Arc::clone(&directory),
                pending_commands,
                snd_tx,
                dispatch_tx,
                _transport: PhantomData,
            });

            // Start sender
            let tx_handle = tokio::spawn(sender.run());

            // Start receiver
            let rx_handle = tokio::spawn(receiver.run(core.clone()));

            // Transition to started state
            let mut state_lock = self.inner.lock().unwrap();
            *state_lock = Some(State::Started {
                core,
                configuration,
                directory,
                peer_id,
                environment,
                cancellation,
                tx_handle,
                rx_handle,
            });

            Ok(())
        } else {
            let mut state_lock = self.inner.lock().unwrap();
            *state_lock = state;
            Err(Error::InvalidOperation)
        }
    }

    async fn stop(&self) -> Result<()> {
        // Take the state so that we do not hold a `MutexGuard` across await points,
        // which is not `Send`
        // TODO(oktal): revert back to the original state if the function errors
        let state = self.inner.lock().unwrap().take();

        info!("stopping bus...");

        let (inner, res) = match state {
            Some(State::Started {
                core,
                configuration,
                directory,
                peer_id,
                environment,
                cancellation,
                tx_handle,
                rx_handle,
            }) => {
                // Unregister from directory
                core.unregister().await?;

                // Cancel receiver and sender
                cancellation.cancel();

                // Wait for the receiver to stop and yield us back the dispatcher
                let rx_handle = rx_handle.await.map_err(Error::Join)?;

                // Wait for the sender to stop and yield us back the transport
                let tx_handle = tx_handle.await.map_err(Error::Join)?;

                // Stop the dispatcher
                let mut dispatcher = rx_handle.dispatcher;
                dispatcher.stop().map_err(Error::Dispatch)?;

                // Stop the transport
                let mut transport = tx_handle.transport;
                transport.stop().map_err(|e| Error::Transport(e.into()))?;

                // Transition to configured state
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

        *self.inner.lock().unwrap() = inner;
        info!("... bus stopped");
        res
    }

    async fn send(&self, command: &dyn Command) -> Result<CommandFuture> {
        let core = self.core()?;
        core.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: crate::Peer) -> Result<CommandFuture> {
        let core = self.core()?;
        core.send_to(command, peer).await
    }
    async fn publish(&self, event: &dyn Event) -> Result<()> {
        let core = self.core()?;
        core.publish(event).await
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

    async fn publish(&self, event: &dyn Event) -> Result<()> {
        self.publish(event).await
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

    pub fn handles<H>(mut self, handler: H) -> Self
    where
        H: InvokerService + 'static,
    {
        self.dispatcher.add(Box::new(handler)).unwrap();
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
    use crate::directory::commands::PingPeerCommand;
    use crate::directory::events::PeerSubscriptionsForTypeUpdated;
    use crate::directory::{
        PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
    };
    use crate::dispatch::router::{RouteHandler, Router};
    use crate::inject::{self, State};
    use crate::message_type_id::MessageTypeId;
    use crate::{
        directory::{
            commands::{RegisterPeerCommand, RegisterPeerResponse},
            event::PeerEvent,
            DirectoryReader,
        },
        MessageDescriptor, MessageType,
    };
    use tokio::sync::{broadcast, Notify};

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

        /// Waiting transmission queue
        tx_wait_queue: HashMap<MessageType, Arc<Notify>>,

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

    /// State of the memory directory
    struct MemoryDirectoryState {
        /// Sender channel for peer events
        events_tx: broadcast::Sender<PeerEvent>,

        /// Collection of messages that have been handled by the directory, indexed by their
        /// message type
        messages: HashMap<&'static str, Vec<Arc<dyn Any + Send + Sync>>>,

        /// Collection of peers that have been configured to handle a type of message
        peers: HashMap<&'static str, Vec<Peer>>,
    }

    impl MemoryDirectoryState {
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
        state: Arc<Mutex<MemoryDirectoryState>>,
    }

    impl MemoryDirectory {
        /// Get a list of messages handled by the directory
        fn get_handled<M: MessageDescriptor + Send + Sync + 'static>(&self) -> Vec<Arc<M>> {
            let state = self.state.lock().unwrap();

            match state.messages.get(M::name()) {
                Some(entry) => entry
                    .iter()
                    .filter_map(|m| m.clone().downcast::<M>().ok())
                    .collect(),
                None => vec![],
            }
        }

        /// Add a list of [`peers`] that should handle the message of type [`M`]
        fn add_peers_for<M: MessageDescriptor>(
            &self,
            peers: impl IntoIterator<Item = Peer>,
        ) -> &Self {
            for peer in peers {
                self.add_peer_for::<M>(peer);
            }
            self
        }

        /// Add a peer that should hande the `Message`
        fn add_peer_for<M: MessageDescriptor>(&self, peer: Peer) -> &Self {
            let mut state = self.state.lock().unwrap();
            state.add_peer_for::<M>(peer);
            self
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
                    tx_wait_queue: HashMap::new(),
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

        /// Wait for [`count`] messages of type `M` to be sent through the transport
        async fn wait_for<M: MessageDescriptor + prost::Message + Default>(
            &self,
            count: usize,
        ) -> Vec<(M, Vec<Peer>)> {
            loop {
                // First attempt to retrieve messages from the tx_queue
                let tx_messages = self.get::<M>();

                // If there is enough messages already in the tx_queue, return right away
                if tx_messages.len() >= count {
                    return tx_messages;
                }

                // We need to wait for more messages to be sent through the transport
                let notify = {
                    let mut inner = self.inner.lock().unwrap();

                    inner
                        .tx_wait_queue
                        .entry(MessageType::of::<M>())
                        .or_insert(Arc::new(Notify::new()))
                        .clone()
                };

                notify.notified().await;
            }
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

        fn environment(&self) -> std::result::Result<Cow<'_, str>, Self::Err> {
            unimplemented!();
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

            let msg_type = MessageType::from(message.message_type_id.clone());
            inner.tx_queue.push((message, peers));

            // Notify any waiter that some messages have been sent
            if let Some(notify) = inner.tx_wait_queue.get(&msg_type) {
                notify.notify_waiters();
            }

            Ok(())
        }
    }

    impl DirectoryReader for MemoryDirectory {
        fn get(&self, _peer_id: &PeerId) -> Option<Peer> {
            None
        }

        fn get_peers_handling(&self, message: &dyn Message) -> Vec<Peer> {
            let state = self.state.lock().unwrap();
            if let Some(peers) = state.peers.get(message.name()) {
                peers.clone()
            } else {
                vec![]
            }
        }
    }

    async fn peer_started(
        msg: PeerStarted,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn peer_stopped(
        msg: PeerStopped,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn peer_decommissioned(
        msg: PeerDecommissioned,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn peer_not_responding(
        msg: PeerNotResponding,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn peer_responding(
        msg: PeerResponding,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn ping_peer(
        msg: PingPeerCommand,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    async fn peer_subscriptions_for_type_updated(
        msg: PeerSubscriptionsForTypeUpdated,
        inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
    ) {
        state.lock().unwrap().add_handled(msg)
    }

    impl Directory for MemoryDirectory {
        type EventStream = crate::sync::stream::BroadcastStream<PeerEvent>;
        type Handler = Router<Arc<Mutex<MemoryDirectoryState>>>;

        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: Arc::new(Mutex::new(MemoryDirectoryState::new())),
            })
        }
        fn subscribe(&self) -> Self::EventStream {
            let state = self.state.lock().unwrap();
            state.subscribe().into()
        }

        fn handle_registration(&self, response: RegisterPeerResponse) {
            let mut state = self.state.lock().unwrap();
            state.add_handled(response);
        }

        fn handler(&self) -> Self::Handler {
            Router::with_state(Arc::clone(&self.state))
                .handles(peer_started.into_handler())
                .handles(peer_stopped.into_handler())
                .handles(peer_decommissioned.into_handler())
                .handles(peer_not_responding.into_handler())
                .handles(peer_responding.into_handler())
                .handles(ping_peer.into_handler())
                .handles(peer_subscriptions_for_type_updated.into_handler())
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
        fn with_handler<S>(
            configuration: BusConfiguration,
            state: S,
            route_fn: impl FnOnce(Router<S>) -> Router<S>,
        ) -> Self
        where
            S: Clone + Send + 'static,
        {
            let peer = Peer::test();
            let transport = MemoryTransport::new(peer.clone());

            let environment = "Test".to_string();
            let directory = MemoryDirectory::new();

            let router = Router::with_state(state);

            let mut dispatcher = MessageDispatcher::new();
            dispatcher
                .add(Box::new(route_fn(router)))
                .expect("dispatcher in invalid state");
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
            Self::with_handler(configuration, (), |router| router)
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

        fn create_test_peers(n: usize) -> Vec<Peer> {
            (0..n).map(|_| Peer::test()).collect()
        }
    }

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test", transient)]
    struct BrewCoffeeCommand {
        #[prost(fixed64, tag = 1)]
        id: u64,
    }

    #[derive(prost::Message, crate::Event, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test", transient)]
    struct CoffeeBrewed {
        #[prost(fixed64, tag = 1)]
        id: u64,
    }

    #[derive(Clone)]
    struct HandlerState<M>
    where
        M: Message,
    {
        tx: std::sync::mpsc::Sender<M>,
    }

    impl<M> HandlerState<M>
    where
        M: Message,
    {
        fn new() -> (Self, std::sync::mpsc::Receiver<M>) {
            let (tx, rx) = std::sync::mpsc::channel();
            (Self { tx }, rx)
        }
        fn send(&self, message: M) {
            self.tx.send(message).unwrap();
        }
    }

    const BOILER_TOO_COLD: (i32, &'static str) = (0xBADBAD, "Boiler is too cold to brew coffee");

    async fn brew_coffee(_cmd: BrewCoffeeCommand) {}
    async fn brew_coffee_state(
        cmd: BrewCoffeeCommand,
        inject::State(state): inject::State<HandlerState<BrewCoffeeCommand>>,
    ) {
        state.send(cmd);
    }

    async fn brew_coffee_boiler_error(
        cmd: BrewCoffeeCommand,
        inject::State(state): inject::State<HandlerState<BrewCoffeeCommand>>,
    ) -> std::result::Result<(), (i32, &'static str)> {
        state.send(cmd);
        Err(BOILER_TOO_COLD)
    }

    async fn coffee_brewed(
        cmd: CoffeeBrewed,
        inject::State(state): inject::State<HandlerState<CoffeeBrewed>>,
    ) {
        state.send(cmd);
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
        // Create fixture with handler
        let mut fixture = Fixture::with_handler(Fixture::configuration(), (), |router| {
            router.handles(brew_coffee.into_handler())
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
        let configuration = Fixture::configuration();
        let (state, rx) = HandlerState::new();

        // Create fixture with dispatch
        let mut fixture = Fixture::with_handler(configuration, state, |router| {
            router.handles(brew_coffee_state.into_handler())
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
    async fn publish_event_will_send_transport_message_to_recipient_peers() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with recipient peers
        let peers = Fixture::create_test_peers(3);
        fixture
            .directory
            .add_peers_for::<CoffeeBrewed>(peers.clone());

        // Publish event
        let res = fixture.bus.publish(&CoffeeBrewed { id: 0xBADC0FFEE }).await;

        // Assert that event was successfully published
        assert_eq!(res.is_ok(), true);

        // Wait for the event to be sent through the bus
        let sent_messages = fixture.transport.wait_for::<CoffeeBrewed>(1).await;
        let (sent_message, sent_peers) = sent_messages.iter().exactly_one().unwrap();

        // Make sure the right message was sent
        assert_eq!(sent_message.id, 0xBADC0FFEE);

        // Make sure that the event was sent to all the recipient peers
        assert_eq!(
            sent_peers
                .iter()
                .sorted_by_key(|i| i.id.value())
                .collect::<Vec<_>>(),
            peers
                .iter()
                .sorted_by_key(|i| i.id.value())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_event_will_dispatch_locally_and_send_to_recipient_peers_with_self() {
        // Create fixture with dispatch
        let configuration = Fixture::configuration();
        let (state, rx) = HandlerState::new();

        let mut fixture = Fixture::with_handler(configuration, state, |router| {
            router.handles(coffee_brewed.into_handler())
        });

        // Configure and start the bus
        assert_eq!(fixture.configure().is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with self and other peer
        fixture
            .directory
            .add_peer_for::<CoffeeBrewed>(fixture.peer.clone());

        let peers = Fixture::create_test_peers(3);
        fixture
            .directory
            .add_peers_for::<CoffeeBrewed>(peers.clone());

        // Publish event
        let res = fixture.bus.publish(&CoffeeBrewed { id: 0xBADC0FFEE }).await;

        // Assert that event was successfully published
        assert_eq!(res.is_ok(), true);

        // Make sure the handler was called
        assert_eq!(rx.recv(), Ok(CoffeeBrewed { id: 0xBADC0FFEE }));

        // Wait for the event to be sent through the bus
        let sent_messages = fixture.transport.wait_for::<CoffeeBrewed>(1).await;
        let (sent_message, sent_peers) = sent_messages.iter().exactly_one().unwrap();

        // Make sure the right message was sent
        assert_eq!(sent_message.id, 0xBADC0FFEE);

        // Make sure that the event was sent to all the recipient peers
        assert_eq!(
            sent_peers
                .iter()
                .sorted_by_key(|i| i.id.value())
                .collect::<Vec<_>>(),
            peers
                .iter()
                .sorted_by_key(|i| i.id.value())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn dispatch_command_locally_with_error_response() {
        let configuration = Fixture::configuration();
        let (state, rx) = HandlerState::new();

        // Create fixture with handler
        let mut fixture = Fixture::with_handler(configuration, state, |router| {
            router.handles(brew_coffee_boiler_error.into_handler())
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
        let configuration = Fixture::configuration();
        let (state, rx) = HandlerState::new();

        // Create fixture with handler
        let mut fixture = Fixture::with_handler(configuration, state, |router| {
            router.handles(brew_coffee_state.into_handler())
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
        let configuration = Fixture::configuration();
        let (state, rx) = HandlerState::new();

        // Create fixture with handler
        let mut fixture = Fixture::with_handler(configuration, state, |router| {
            router.handles(brew_coffee_state.into_handler())
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
