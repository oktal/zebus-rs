use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::Utc;
use dyn_clone::clone_box;
use futures_util::StreamExt;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tower_service::Service;
use tracing::{error, info};

use crate::{
    bus::{BusEvent, CommandResult, Error, RegistrationError, Result, SendError},
    core::{MessagePayload, SubscriptionMode},
    directory::{
        self, Directory, DirectoryReader, DirectoryReaderExt, PeerDescriptor, Registration,
    },
    dispatch::{
        self, DispatchOutput, DispatchRequest, DispatchService, Dispatched, MessageDispatcher,
    },
    proto::FromProtobuf,
    sync::stream::EventStream,
    transport::{MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    BindingKey, BusConfiguration, Command, CommandError, Event, Message, MessageExt, MessageId,
    Peer, PeerId, Subscription,
};

struct Core<T: Transport> {
    self_peer: Peer,
    environment: String,

    directory: Arc<dyn DirectoryReader>,

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

        event_tx: EventStream<BusEvent>,

        directory: Arc<D>,
        peer_id: PeerId,
        environment: String,
    },

    Started {
        core: Arc<Core<T>>,

        configuration: BusConfiguration,
        event_tx: EventStream<BusEvent>,
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
    ) -> CommandResult {
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

        // Await the result of the command
        match rx.await {
            Ok(r) => r,
            Err(e) => Err(CommandError::Receive(e)),
        }
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
                .map_err(|e| Error::Transport(e.into()))?
                .await
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

struct Receiver<T: Transport> {
    rcv_rx: T::MessageStream,
    dispatch_rx: mpsc::Receiver<LocalDispatchRequest>,
    tx: mpsc::Sender<SendEntry>,
    self_peer: Peer,
    environment: String,
    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    directory: Arc<dyn DirectoryReader>,
    directory_rx: directory::EventStream,
    event_tx: EventStream<BusEvent>,
    dispatcher: MessageDispatcher,
    cancellation: CancellationToken,
}

impl<T: Transport> Receiver<T> {
    fn new<D: Directory>(
        transport_rx: T::MessageStream,
        tx: mpsc::Sender<SendEntry>,
        self_peer: Peer,
        environment: String,
        directory: Arc<D>,
        event_tx: EventStream<BusEvent>,
        dispatcher: MessageDispatcher,
        cancellation: CancellationToken,
    ) -> (
        Self,
        mpsc::Sender<LocalDispatchRequest>,
        Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<CommandResult>>>>,
    ) {
        let pending_commands = Arc::new(Mutex::new(HashMap::new()));
        let (dispatch_tx, dispatch_rx) = mpsc::channel(128);

        let directory_rx = Box::pin(directory.subscribe());
        let directory = directory.reader();

        (
            Self {
                rcv_rx: transport_rx,
                dispatch_rx,
                tx,
                self_peer,
                environment,
                pending_commands: pending_commands.clone(),
                directory,
                directory_rx,
                event_tx,
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

                Some(peer_event) = self.directory_rx.next() => {
                    let _ = self.event_tx.send(BusEvent::Peer(peer_event));
                }

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
                            match self.dispatch(DispatchRequest::local(message, bus.clone())).await {
                                Ok(dispatched) => {
                                    // Retrieve the `CommandResult`
                                    let command_result: Option<CommandResult> = dispatched.try_into().ok();

                                    // Resolve command future
                                    if let Some(command_result) = command_result {
                                        let _ = tx.send(command_result);
                                    }

                                },
                                Err(e) => error!("failed to dispatch message locally: {e}")
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
        let dst_peers = self.directory.get_peers_handling_message_val(message);
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
    descriptor: PeerDescriptor,
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
            descriptor.clone(),
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

impl<T: Transport> Core<T> {
    async fn send(&self, command: &dyn Command) -> CommandResult {
        // Retrieve the list of peers handling the command from the directory
        let peers = self.directory.get_peers_handling_message_val(command.up());

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

            // Await the result of the command
            match rx.await {
                Ok(r) => r,
                Err(e) => Err(CommandError::Receive(e)),
            }
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
                Err(Error::Send(SendError::PeerNotResponding(dst_peer)).into())
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

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> CommandResult {
        // Attempting to send a non-persistent command to a non-responding peer result in
        // an error
        if !peer.is_responding && command.is_transient() && !command.is_infrastructure() {
            return Err(Error::Send(SendError::PeerNotResponding(peer)).into());
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
        let peers = self.directory.get_peers_handling_message_val(event.up());
        let len = peers.len();

        let name = event.name();

        if len == 0 {
            info!(message = event.name(), "sending {name} to no target peer");
            return Ok(());
        }

        if len == 1 {
            let peer = &peers[0];
            info!(message = event.name(), "sending {name} to {peer}");
        } else {
            let first = &peers[0];
            let rest = len - 1;
            info!(
                message = event.name(),
                "sending {name} to {first} and {rest} other peers"
            );
        }

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
impl<T: Transport> crate::Bus for Core<T> {
    async fn configure(&self, _peer_id: PeerId, _environment: String) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn start(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn stop(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn send(&self, command: &dyn Command) -> CommandResult {
        self.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> CommandResult {
        self.send_to(command, peer).await
    }

    async fn publish(&self, event: &dyn Event) -> Result<()> {
        self.publish(event).await
    }
}

pub(super) struct Bus<T: Transport, D: Directory> {
    inner: tokio::sync::Mutex<Option<State<T, D>>>,
}

impl<T: Transport, D: Directory> Bus<T, D> {
    pub(super) fn new(
        configuration: BusConfiguration,
        transport: T,
        directory: Arc<D>,
        dispatcher: MessageDispatcher,
    ) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Some(State::Init {
                configuration,
                transport,
                directory,
                dispatcher,
            })),
        }
    }
}

impl<T: Transport, D: Directory> Bus<T, D> {
    async fn configure(&self, peer_id: PeerId, environment: String) -> Result<()> {
        let mut state = self.inner.lock().await;

        let (inner, res) = match state.take() {
            Some(State::Init {
                configuration,
                mut transport,
                directory,
                dispatcher,
            }) => {
                // Create event stream for bus events
                let event = EventStream::new(32);

                // Create directory reader
                let directory_reader = directory.reader();

                // Configure transport
                transport
                    .configure(
                        peer_id.clone(),
                        environment.clone(),
                        directory_reader,
                        event.clone(),
                    )
                    .map_err(|e| Error::Transport(e.into()))?;

                (
                    Some(State::Configured {
                        configuration,
                        transport,
                        dispatcher,
                        event_tx: event,
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

        let mut state = self.inner.lock().await;
        let (inner, res) = match state.take() {
            Some(State::Configured {
                configuration,
                mut transport,
                mut dispatcher,
                event_tx,
                peer_id,
                directory,
                environment,
            }) => {
                if let Err(e) = event_tx.send(BusEvent::Starting) {
                    error!("failed to publish {:?}", e.0);
                }

                // Start transport
                let start_future = transport.start().map_err(|e| Error::Transport(e.into()))?;

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

                // Create description of our current peer
                let utc_now = Utc::now();
                let descriptor = PeerDescriptor {
                    peer: self_peer.clone(),
                    subscriptions: startup_subscriptions,
                    is_persistent: configuration.is_persistent,
                    timestamp_utc: Some(utc_now.into()),
                    has_debugger_attached: Some(false),
                };

                if let Err(e) = event_tx.send(BusEvent::Registering(descriptor.clone())) {
                    error!("failed to publish {:?}", e.0);
                }

                // Register to directory
                let registration =
                    register(&mut transport, descriptor, &environment, &configuration).await?;

                // Handle peer directory response
                // TODO(oktal): properly handle error
                if let Ok(response) = registration.result {
                    let peers = response
                        .peers
                        .clone()
                        .into_iter()
                        .map(PeerDescriptor::from_protobuf)
                        .collect();

                    if let Err(e) = event_tx.send(BusEvent::Registered(peers)) {
                        error!("failed to publish {:?}", e.0);
                    }

                    directory.handle_registration(response);
                }

                // Start the dispatcher
                dispatcher.start().map_err(Error::Dispatch)?;

                // Wait for transport to start
                start_future.await.map_err(|e| Error::Transport(e.into()))?;

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
                let (receiver, dispatch_tx, pending_commands) = Receiver::<T>::new(
                    rcv_rx,
                    snd_tx.clone(),
                    self_peer.clone(),
                    environment.clone(),
                    directory.clone(),
                    event_tx.clone(),
                    dispatcher,
                    cancellation.clone(),
                );

                // Create Core
                let core = Arc::new(Core::<T> {
                    self_peer,
                    environment: environment.clone(),
                    directory: directory.reader(),
                    pending_commands,
                    snd_tx,
                    dispatch_tx,
                    _transport: PhantomData,
                });

                // Start sender
                let tx_handle = tokio::spawn(sender.run());

                // Start receiver
                let rx_handle = tokio::spawn(receiver.run(core.clone()));

                if let Err(e) = event_tx.send(BusEvent::Started) {
                    error!("failed to publish {:?}", e.0);
                }

                // Transition to started state
                (
                    Some(State::Started {
                        core,
                        configuration,
                        event_tx,
                        directory,
                        peer_id,
                        environment,
                        cancellation,
                        tx_handle,
                        rx_handle,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        *state = inner;
        res
    }

    async fn stop(&self) -> Result<()> {
        let mut state = self.inner.lock().await;

        info!("stopping bus...");

        let (inner, res) = match state.take() {
            Some(State::Started {
                core,
                configuration,
                event_tx,
                directory,
                peer_id,
                environment,
                cancellation,
                tx_handle,
                rx_handle,
                ..
            }) => {
                if let Err(e) = event_tx.send(BusEvent::Stopping) {
                    error!("failed to publish {:?}", e.0);
                }

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
                let stop_future = transport.stop().map_err(|e| Error::Transport(e.into()))?;

                if let Err(e) = event_tx.send(BusEvent::Stopped) {
                    error!("failed to publish {:?}", e.0);
                }

                // Wait for transport to stop
                stop_future.await.map_err(|e| Error::Transport(e.into()))?;

                // Transport to Configured state
                (
                    Some(State::Configured {
                        configuration,
                        transport,
                        dispatcher,
                        event_tx,
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
        info!("... bus stopped");
        res
    }

    async fn send(&self, command: &dyn Command) -> CommandResult {
        let core = self.core().await?;
        core.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: crate::Peer) -> CommandResult {
        let core = self.core().await?;
        core.send_to(command, peer).await
    }
    async fn publish(&self, event: &dyn Event) -> Result<()> {
        let core = self.core().await?;
        core.publish(event).await
    }

    async fn core(&self) -> Result<Arc<Core<T>>> {
        let inner = self.inner.lock().await;
        match inner.as_ref() {
            Some(State::Started { core, .. }) => Ok(core.clone()),
            _ => Err(Error::InvalidOperation),
        }
    }
}

#[async_trait]
impl<T: Transport, D: Directory> crate::Bus for Bus<T, D> {
    async fn configure(&self, peer_id: PeerId, environment: String) -> Result<()> {
        self.configure(peer_id, environment).await
    }

    async fn start(&self) -> Result<()> {
        self.start().await
    }

    async fn stop(&self) -> Result<()> {
        self.stop().await
    }

    async fn send(&self, command: &dyn Command) -> CommandResult {
        self.send(command).await
    }

    async fn send_to(&self, command: &dyn Command, peer: Peer) -> CommandResult {
        self.send_to(command, peer).await
    }

    async fn publish(&self, event: &dyn Event) -> Result<()> {
        self.publish(event).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::bus::CommandError;
    use crate::directory::commands::{RegisterPeerCommand, RegisterPeerResponse};
    use crate::directory::memory::MemoryDirectory;
    use crate::dispatch::router::{RouteHandler, Router};
    use crate::inject::{self};
    use crate::message_type_id::MessageTypeId;
    use crate::proto::IntoProtobuf;
    use crate::transport::memory::MemoryTransport;

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

        async fn configure(&mut self) -> std::result::Result<(), Error> {
            self.bus
                .configure(self.peer.id.clone(), self.environment.clone())
                .await
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
                        command_id: message.id.into_protobuf(),
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

    #[tokio::test]
    async fn configure_bus_will_configure_transport() {
        let mut fixture = Fixture::new_default();

        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);

        // Attempt to send a command without starting the bus
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with InvalidOperation
        assert!(matches!(
            res,
            Err(CommandError::Bus(Error::InvalidOperation))
        ));
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
        fixture.configure().await.expect("Failed to configure bus");

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
                    command_id: message.id.into_protobuf(),
                    error_code: 0,
                    payload_type_id: Some(message_type.into_protobuf()),
                    payload: Some(response.encode_to_vec()),
                    response_message: None,
                }
            },
        );

        // Basic assertions
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);
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
                    command_id: message.id.into_protobuf(),
                    error_code: 0,
                    payload_type_id: Some(message_type.into_protobuf()),
                    payload: Some(response.encode_to_vec()),
                    response_message: None,
                }
            },
        );

        // Basic assertions
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Attempt to send a command with empty directory
        let res = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that send failed with NoPeer
        assert!(matches!(
            res,
            Err(CommandError::Bus(Error::Send(SendError::NoPeer)))
        ));
    }

    #[tokio::test]
    async fn send_command_returns_error_if_multiple_peers_handle_command() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert!(matches!(
            res,
            Err(CommandError::Bus(Error::Send(SendError::MultiplePeers(_))))
        ));
    }

    #[tokio::test]
    async fn send_command_returns_error_if_transient_and_peer_not_responding() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().await.is_ok(), true);
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
            Err(CommandError::Bus(Error::Send(
                SendError::PeerNotResponding(_)
            )))
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
        assert_eq!(fixture.configure().await.is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with self and other peer
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(Peer::test());

        // Send command
        let result = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

        // Assert that command has been locally dispatched
        assert!(matches!(result, Ok(None)));

        // Make sure the handler was called
        assert_eq!(rx.try_recv(), Ok(BrewCoffeeCommand { id: 0xBADC0FFEE }));
    }

    #[tokio::test]
    async fn publish_event_will_send_transport_message_to_recipient_peers() {
        let mut fixture = Fixture::new_default();

        // Configure and start the bus
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);
        assert_eq!(fixture.start_with_registration().await.is_ok(), true);

        // Setup directory with self
        fixture
            .directory
            .add_peer_for::<BrewCoffeeCommand>(fixture.peer.clone());

        // Send command
        let result = fixture
            .bus
            .send(&BrewCoffeeCommand { id: 0xBADC0FFEE })
            .await;

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
        assert_eq!(fixture.configure().await.is_ok(), true);
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
        assert_eq!(fixture.configure().await.is_ok(), true);
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
