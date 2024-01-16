pub trait ConfigurationProvider {
    type Configuration;

    fn configure(&mut self) -> Self::Configuration;
}
