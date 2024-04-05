//! Provides a builder to configure and create a [`Bus`]
use crate::{
    bus,
    configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    directory::{self, Directory},
    dispatch::{InvokerService, MessageDispatcher},
    transport::{Transport, TransportExt},
    Bus, BusConfiguration, ConfigurationProvider, PeerId,
};

/// Represents an intermerdiate step when building a bus
pub trait Step {}

/// Initial [`Step`]
pub struct Init;

/// Bus has been configured
pub struct Configured {
    configuration: BusConfiguration,
    peer_id: PeerId,
    environment: String,
    dispatcher: MessageDispatcher,
}

/// [`Transport`] layer has been attached to the [`Bus`]
pub struct TransportAttached<T: Transport> {
    configuration: BusConfiguration,
    peer_id: PeerId,
    environment: String,
    dispatcher: MessageDispatcher,
    transport: T,
}

impl Step for Init {}

/// A builder pattern struct used to configure and construct instances of [`Bus`].
///
/// It allows for flexible customization of the [`Bus`] instance before it's created.
pub struct BusBuilder<Step = Init> {
    step: Step,
}

impl BusBuilder<Init> {
    /// Create a new default builder
    pub fn new() -> Self {
        Self { step: Init }
    }

    /// Configure the bus to use the given peer id, environment and register to the given directory endpoints.
    ///
    /// Use the default configuration parameter values for the rest of the [`BusConfiguration`]
    pub fn configure_default<P, DirectoryEndoint, Endpoint, Env>(
        self,
        peer_id: P,
        environment: Env,
        directory_endpoints: DirectoryEndoint,
    ) -> BusBuilder<Configured>
    where
        P: Into<PeerId>,
        DirectoryEndoint: IntoIterator<Item = Endpoint>,
        Endpoint: AsRef<str>,
        Env: Into<String>,
    {
        let directory_endpoints = directory_endpoints
            .into_iter()
            .map(|i| i.as_ref().to_string())
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

        self.configure(peer_id, environment, configuration)
    }

    /// Configure the bus to use the given peer id and environment.
    ///
    /// Use  the provided [`ConfigurationProvider`] to retrieve the [`BusConfiguration`]
    pub fn configure_with<P, Provider, Env>(
        self,
        peer_id: P,
        environment: Env,
        provider: &mut Provider,
    ) -> bus::Result<BusBuilder<Configured>>
    where
        P: Into<PeerId>,
        Provider: ConfigurationProvider,
        <Provider as ConfigurationProvider>::Configuration: Into<BusConfiguration>,
        Env: Into<String>,
    {
        let configuration = provider
            .configure()
            .map_err(|e| bus::Error::Configuration(e.into()))?
            .into();
        Ok(self.configure(peer_id, environment.into(), configuration))
    }

    /// Configure the bus to use the given peer id, environment and [`BusConfiguration`]
    pub fn configure<P, Env>(
        self,
        peer_id: P,
        environment: Env,
        configuration: BusConfiguration,
    ) -> BusBuilder<Configured>
    where
        P: Into<PeerId>,
        Env: Into<String>,
    {
        let environment = environment.into();
        let peer_id = peer_id.into();

        BusBuilder::<Configured> {
            step: Configured {
                configuration,
                peer_id,
                environment,
                dispatcher: MessageDispatcher::new(),
            },
        }
    }
}

impl BusBuilder<Configured> {
    /// Attach a [`Transport`] layer to the bus
    pub fn with_transport<T>(self, transport: T) -> BusBuilder<TransportAttached<T>>
    where
        T: Transport,
    {
        let step = self.step;
        BusBuilder::<TransportAttached<T>> {
            step: TransportAttached {
                configuration: step.configuration,
                peer_id: step.peer_id,
                environment: step.environment,
                dispatcher: step.dispatcher,
                transport,
            },
        }
    }

    pub fn handles<H>(mut self, handler: H) -> Self
    where
        H: InvokerService + 'static,
    {
        self.step.dispatcher.add(Box::new(handler)).unwrap();
        self
    }
}

impl<T> BusBuilder<TransportAttached<T>>
where
    T: Transport,
{
    /// Create the final [`Bus`]
    pub async fn create(self) -> std::result::Result<impl Bus, bus::Error> {
        let configuration = self.step.configuration;
        let peer_id = self.step.peer_id;
        let environment = self.step.environment;
        let dispatcher = self.step.dispatcher;
        let transport = self.step.transport;

        // Create peer directory client
        let client = directory::Client::new();

        // Create the bus
        let bus = super::bus::Bus::new(
            configuration.clone(),
            transport.persistent(configuration),
            client,
            dispatcher,
        );

        // Configure the bus
        bus.configure(peer_id, environment).await?;
        Ok(bus)
    }
}
