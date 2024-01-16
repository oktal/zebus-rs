use crate::BoxError;

pub trait ConfigurationProvider {
    type Configuration;
    type Error: Into<BoxError>;

    fn configure(&mut self) -> Result<Self::Configuration, Self::Error>;
}
