use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    bus::{CommandFuture, CommandResult},
    bus_configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    directory::{self, Registration},
    transport::{self, SendContext, Transport, TransportMessage},
    Bus, BusConfiguration, Command, Peer, PeerId,
};

struct CommandPromise(tokio::sync::oneshot::Sender<CommandResult>);

#[derive(Debug)]
pub enum Error<E> {
    /// Transport error
    Transport(E),

    /// None of the directories tried for registration succeeded
    Registration,

    /// An operation was attempted while the [`Bus`] was in an invalid state
    InvalidOperation,
}

enum State<T: Transport> {
    Init {
        runtime: tokio::runtime::Runtime,
        configuration: BusConfiguration,
        transport: T,
    },

    Configured {
        runtime: tokio::runtime::Runtime,
        configuration: BusConfiguration,
        transport: T,
        peer_id: PeerId,
        environment: String,
    },

    Started {
        runtime: tokio::runtime::Runtime,
        configuration: BusConfiguration,
        transport: T,
        self_peer: Peer,
        environment: String,

        pending_commands: Arc<Mutex<HashMap<uuid::Uuid, CommandPromise>>>,
        rx_handle: tokio::task::JoinHandle<()>,
    },
}

async fn receive(
    mut receiver: transport::Receiver,
    pending_commands: Arc<Mutex<HashMap<uuid::Uuid, CommandPromise>>>,
) {
    let message = receiver.recv().await;
}

fn try_register<T: Transport>(
    transport: &mut T,
    receiver: &transport::Receiver,
    runtime: &tokio::runtime::Runtime,
    self_peer: Peer,
    environment: String,
    configuration: &BusConfiguration,
) -> Result<Registration, Error<T::Err>> {
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

    let registration_timeout = configuration.registration_timeout;

    for directory_peer in directory_peers {
        // Safety: since we are blocking on the future, we are guaranteed that the `receiver` will
        // live long enough
        let registration = unsafe {
            directory::register(
                transport,
                receiver,
                self_peer.clone(),
                environment.clone(),
                directory_peer,
            )
        }
        .map_err(Error::Transport)?;

        match runtime.block_on(async { registration.with_timeout(registration_timeout).await }) {
            Ok(registration) => return Ok(registration),
            Err(_) => {}
        }
    }

    Err(Error::Registration)
}

struct BusImpl<T: Transport> {
    inner: Option<State<T>>,
}

impl<T: Transport> BusImpl<T> {
    fn new(
        runtime: tokio::runtime::Runtime,
        configuration: BusConfiguration,
        transport: T,
    ) -> Self {
        Self {
            inner: Some(State::Init {
                runtime,
                configuration,
                transport,
            }),
        }
    }
}

impl<T: Transport> Bus for BusImpl<T> {
    type Err = Error<T::Err>;

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(State::Init {
                runtime,
                configuration,
                mut transport,
            }) => {
                transport
                    .configure(peer_id.clone(), environment.clone())
                    .map_err(Error::Transport)?;

                (
                    Some(State::Configured {
                        runtime,
                        configuration,
                        transport,
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
                peer_id,
                environment,
            }) => {
                // Start transport
                let receiver = transport.start().map_err(Error::Transport)?;
                let endpoint = transport.inbound_endpoint().map_err(Error::Transport)?;

                // Register to directory
                let self_peer = Peer {
                    id: peer_id.clone(),
                    endpoint: endpoint.to_string(),
                    is_up: true,
                    is_responding: true,
                };

                let registration = try_register(
                    &mut transport,
                    &receiver,
                    &runtime,
                    self_peer.clone(),
                    environment.clone(),
                    &configuration,
                )?;
                println!("Successfully registered {registration:?}");

                let pending_commands = Arc::new(Mutex::new(HashMap::new()));
                let rx_handle = runtime.spawn(receive(receiver, Arc::clone(&pending_commands)));

                // Transition to started state
                (
                    Some(State::Started {
                        runtime,
                        configuration,
                        transport,
                        self_peer,
                        environment,
                        pending_commands,
                        rx_handle,
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
                ref mut transport,
                ref pending_commands,
                ref self_peer,
                ref environment,
                ..
            }) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let (id, message) =
                    TransportMessage::create(&self_peer, environment.clone(), command);

                let mut pending_commands = pending_commands.lock().unwrap();
                pending_commands.entry(id).or_insert(CommandPromise(tx));

                transport.send(std::iter::once(peer), message, SendContext::default());
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
    runtime: Option<tokio::runtime::Runtime>,
}

impl<T: Transport> BusBuilder<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            peer_id: None,
            configuration: None,
            environment: None,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: tokio::runtime::Runtime) -> Self {
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

    pub fn create(self) -> Result<impl Bus, CreateError<Error<T::Err>>> {
        let (transport, peer_id, configuration, environment) = (
            self.transport,
            self.peer_id.unwrap_or(Self::testing_peer_id()),
            self.configuration
                .ok_or(CreateError::MissingConfiguration)?,
            self.environment.ok_or(CreateError::MissingConfiguration)?,
        );

        // Create tokio runtime
        let runtime = self.runtime.unwrap_or(Self::default_runtime());

        let mut bus = BusImpl::new(runtime, configuration, transport);
        bus.configure(peer_id, environment)
            .map_err(CreateError::Configure)?;
        Ok(bus)
    }

    fn default_runtime() -> tokio::runtime::Runtime {
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
