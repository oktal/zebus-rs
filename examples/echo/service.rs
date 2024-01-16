use std::error::Error;
use std::io::{self, Read};
use tokio;
use zebus::configuration::DefaultConfigurationProvider;
use zebus::dispatch::{RouteHandler, Router};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusConfiguration, BusBuilder, Command, ConfigurationProvider, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use tracing::{error, info};

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Abc.Echo")]
pub struct EchoCommand {
    #[prost(string, required, tag = 1)]
    msg: String,
}

async fn echo(cmd: EchoCommand) {
    info!("{}", cmd.msg);
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

    let mut configuration = DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/echo/bus.toml");

    let bus = BusBuilder::new()
        .configure_with(
            PeerId::new("Abc.Echo.0"),
            "example",
            &mut configuration
        )?
        .handles(Router::with_state(()).handles(echo.into_handler()))
        .with_transport(zmq)
        .create()
        .unwrap();

    if let Err(e) = bus.start().await {
        error!("failed to start bus: {e}");
    } else {
        println!("Press a key to exit");
        io::stdin()
            .bytes()
            .next()
            .expect("Failed to read input")
            .unwrap();

        bus.stop().await.expect("Failed to stop bus");
    }

    Ok(())
}
