use std::error::Error;
use std::io;
use std::time::Duration;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use tracing::{error, info};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder, Command, PeerId};

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

    let zmq_configuration = ZmqTransportConfiguration {
        inbound_endpoint: "tcp://*:*".into(),
        wait_for_end_of_stream_ack_timeout: Duration::from_secs(10),
    };

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let bus = BusBuilder::new()
        .configure_default(
            PeerId::new_unique("Abc.Echo.Client"),
            "example",
            ["tcp://localhost:7465"],
        )
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
