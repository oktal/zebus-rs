use std::error::Error;
use std::io::{self, Read};
use std::time::Duration;
use tokio;
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use tracing::error;

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

    let bus = BusBuilder::new(zmq)
        .with_default_configuration("tcp://localhost:7465", "test".to_string())
        .create()
        .unwrap();

    if let Err(e) = bus.start().await {
        error!("{e}");
    }

    println!("Press a key to exit");
    io::stdin()
        .bytes()
        .next()
        .expect("Failed to read input")
        .unwrap();

    bus.stop().await.expect("Failed to stop bus");

    Ok(())
}
