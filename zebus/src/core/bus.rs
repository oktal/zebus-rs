use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

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
    directory::{
        self, commands::PingPeerCommand, event::PeerEvent, PeerDecommissioned, PeerNotResponding,
        PeerResponding, PeerStarted, PeerStopped, Registration,
    },
    dispatch::{self, Dispatched, Dispatcher, MessageDispatcher},
    transport::{self, SendContext, Transport, TransportMessage},
    Bus, BusConfiguration, Command, Handler, Peer, PeerId,
};

struct CommandPromise(oneshot::Sender<CommandResult>);

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
pub enum Error {
    /// Transport error
    #[error("an error occured during a transport operation {0}")]
    Transport(Box<dyn std::error::Error>),

    /// None of the directories tried for registration succeeded
    #[error("{0}")]
    Registration(RegistrationError),

    /// An error occured on the dispatcher
    #[error("an error occured on the dispatcher {0}")]
    Dispatch(dispatch::Error),

    /// An operation was attempted while the [`Bus`] was in an invalid state
    #[error("n operation was attempted while the bus was not in a valid state")]
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

        peer_id: PeerId,
        environment: String,
    },

    Started {
        runtime: Arc<Runtime>,
        configuration: BusConfiguration,
        self_peer: Peer,
        environment: String,

        directory: directory::Client,

        pending_commands: Arc<Mutex<HashMap<uuid::Uuid, CommandPromise>>>,
        snd_tx: mpsc::Sender<SendEntry>,
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

    fn send(&mut self, entry: SendEntry) {
        self.transport.send(
            entry.peers.into_iter(),
            entry.message,
            SendContext::default(),
        );
    }
}

struct ReceiverContext {
    rx: transport::Receiver,
    tx: mpsc::Sender<SendEntry>,
    self_peer: Peer,
    environment: String,
    dispatcher: MessageDispatcher,
}

impl ReceiverContext {
    fn new(
        rx: transport::Receiver,
        tx: mpsc::Sender<SendEntry>,
        self_peer: Peer,
        environment: String,
        dispatcher: MessageDispatcher,
    ) -> Self {
        Self {
            rx,
            tx,
            self_peer,
            environment,
            dispatcher,
        }
    }

    async fn dispatch(&mut self, message: TransportMessage) -> Dispatched {
        self.dispatcher.dispatch(message).await
    }

    async fn send(&mut self, message: TransportMessage, peers: Vec<Peer>) {
        self.tx.send(SendEntry { message, peers }).await;
    }
}

/// Reception loop for [`TransportMesssage`] messages
async fn receiver(mut ctx: ReceiverContext) {
    while let Some(message) = ctx.rx.recv().await {
        // Dispatch message
        let dispatched = ctx.dispatch(message).await;

        // If the message that has been dispatched is a Command, send back the
        // MessageExecutionCompleted
        if dispatched.is_command() {
            let (message, peer) =
                dispatched.into_transport(&ctx.self_peer, ctx.environment.clone());
            ctx.send(message, vec![peer]).await;
        }
    }
}

/// Sender loop for [`TransportMessage`] messages
async fn sender<T: Transport>(mut ctx: SenderContext<T>) {
    while let Some(entry) = ctx.rx.recv().await {
        ctx.send(entry);
    }
}

/// Reception loop for [`PeerEvent`] peer directory events
async fn directory_rx(events_rx: mpsc::Receiver<PeerEvent>) {
    let mut events_rx = events_rx;

    while let Some(event) = events_rx.recv().await {
        println!("{event:?}");
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

                transport
                    .configure(peer_id.clone(), environment.clone(), Arc::clone(&runtime))
                    .map_err(|e| Error::Transport(e.into()))?;

                (
                    Some(State::Configured {
                        runtime,
                        configuration,
                        transport,
                        dispatcher,
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

                // Start peer directory client
                let (mut directory, directory_events_rx) = directory::Client::start();

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
                            .handles::<PingPeerCommand>();
                    }))
                    .map_err(Error::Dispatch)?;

                // Start the dispatcher
                dispatcher.start().map_err(Error::Dispatch)?;

                let _directory_handle = runtime.spawn(directory_rx(directory_events_rx));

                let pending_commands = Arc::new(Mutex::new(HashMap::new()));

                // Create sender and receiver
                let (snd_tx, snd_ctx) = SenderContext::new(transport);
                let rcv_ctx = ReceiverContext::new(
                    transport_receiver,
                    snd_tx.clone(),
                    self_peer.clone(),
                    environment.clone(),
                    dispatcher,
                );

                // Start sender and receiver
                let tx_handle = runtime.spawn(sender(snd_ctx));
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

    fn send<C: Command>(&mut self, command: &C) {
        todo!()
    }

    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: &C,
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
                let (tx, rx) = oneshot::channel();
                let (id, message) =
                    TransportMessage::create(&self_peer, environment.clone(), command);

                let mut pending_commands = pending_commands.lock().unwrap();
                pending_commands.entry(id).or_insert(CommandPromise(tx));

                // TODO(oktal): use async send
                snd_tx.blocking_send(SendEntry {
                    message,
                    peers: vec![peer],
                });
                Ok(CommandFuture(rx))
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
