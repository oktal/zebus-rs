//! Components that can provide configuration
use crate::BoxError;

/// A special trait for a component that can provide a configuration from any configuration source
pub trait ConfigurationProvider {
    /// The type of configuration provided
    type Configuration;

    /// Error type that can occur during configuraiton
    type Error: Into<BoxError>;

    /// Attampt to retrieve the [`Self::Configuration`] provided by this trait
    fn configure(&mut self) -> Result<Self::Configuration, Self::Error>;
}
