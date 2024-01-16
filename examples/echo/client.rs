use std::error::Error;
use std::io;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use tracing::{error, info};
use zebus::configuration::DefaultConfigurationProvider;
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder, BusConfiguration, Command, ConfigurationProvider, PeerId};

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Abc.Echo")]
pub struct EchoCommand {
    #[prost(string, required, tag = 1)]
    msg: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/echo/zmq.toml")
        .configure()?;

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut configuration = DefaultConfigurationProvider::<BusConfiguration>::default()
        .with_file("examples/echo/bus.toml");

    let bus = BusBuilder::new()
        .configure_with(
            PeerId::new_unique("Abc.Echo.Client"),
            "example",
            &mut configuration,
        )?
        .with_transport(zmq)
        .create()
        .unwrap();

    if let Err(e) = bus.start().await {
        error!("failed to start bus: {e}");
    } else {
        println!("Enter text to echo");
        let mut msg = String::new();
        io::stdin().read_line(&mut msg)?;

        info!("Echoing {msg}");

        bus.send(&EchoCommand { msg }).await?;
        bus.stop().await.expect("Failed to stop bus");
    }

    Ok(())
}
