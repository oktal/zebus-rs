use crate::ConfigurationProvider;

use config::{Config, ConfigError, Environment, File};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

#[derive(Default)]
pub struct DefaultConfigurationProvider<T> {
    file: Option<PathBuf>,
    _phantom: PhantomData<T>,
}

impl<T> DefaultConfigurationProvider<T> {
    pub fn with_file(mut self, file: impl AsRef<Path>) -> Self {
        self.file = Some(file.as_ref().into());
        self
    }
}

impl<T> ConfigurationProvider for DefaultConfigurationProvider<T>
where
    T: serde::de::DeserializeOwned,
{
    type Configuration = T;
    type Error = ConfigError;

    fn configure(&mut self) -> Result<Self::Configuration, Self::Error> {
        let file = self
            .file
            .clone()
            .and_then(|file| file.into_os_string().into_string().ok());

        let mut builder = Config::builder();
        if let Some(file) = file {
            builder = builder.add_source(File::with_name(&file))
        }
        let config = builder
            .add_source(
                Environment::with_prefix("ZEBUS")
                    .try_parsing(true)
                    .list_separator(","),
            )
            .build()?;

        config.try_deserialize()
    }
}
