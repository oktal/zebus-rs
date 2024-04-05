//! Provides configuration components for the bus
pub mod bus;
pub mod provider;

pub use bus::{
    BusConfiguration, DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT,
    DEFAULT_START_REPLAY_TIMEOUT,
};
pub use provider::ConfigurationProvider;

#[cfg(feature = "config-provider")]
pub mod default;
#[cfg(feature = "config-provider")]
pub use default::DefaultConfigurationProvider;
