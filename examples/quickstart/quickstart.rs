//! This example demonstrates how to define a command and start a bus with a subscription to a routed event
use std::error::Error;
use std::io::{self, Read};
use tracing::{error, info};
use zebus::configuration::DefaultConfigurationProvider;
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder, BusConfiguration, Command, ConfigurationProvider, Event, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

/// A [`Command`] definition.
/// A command must be a valid protobuf message.
/// [`prost`](https://docs.rs/prost/) is the crate used to serialize and serialize protobuf
/// A [`Command`] or [`Event`] definition must have an associated namespace.
/// The fully qualified named of the command will be `Quickstart.MyCommand`
#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Quickstart")]
pub struct MyCommand {
    /// Commands and events can hold fields like any protobuf message
    /// A sample string field
    #[prost(string, tag = 1)]
    pub key: String,

    /// A second associated integer field
    #[prost(fixed32, tag = 2)]
    pub value: u32,
}

/// An [`Event`] definition.
/// A command must be a valid protobuf message.
/// [`prost`](https://docs.rs/prost/) is the crate used to serialize and serialize protobuf
/// A [`Command`] or [`Event`] definition must have an associated namespace.
/// The fully qualified named of the command will be `Quickstart.MyEvent`
/// The `routable` attribute indicates that this event is routed. A routed event must have at least
/// on field marked with a `routing_position` attribute
#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "Quickstart", routable)]
pub struct MyEvent {
    /// Commands and events can hold fields like any protobuf message
    /// A sample string field
    /// The `key` field is marked as a routing field. When multiple fields are use as a routing key,
    /// each field must have a unique `routing_position`
    #[prost(string, tag = 1)]
    #[zebus(routing_position = 1)]
    pub key: String,

    /// A second associated integer field
    #[prost(fixed32, tag = 2)]
    pub value: u32,
}

/// Message handler for `MyEvent`.
/// A message handler will be called by the bus everytime a new command or event is received
/// A message handler handler must be an asynchronous function, where the first argument is the type of the message
/// being handled
async fn handle_my_event(ev: MyEvent) {
    info!("{}:{}", ev.key, ev.value);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    // Retrieve the configuration for the zmq transport layer from the `examples/zmq.toml` configuration file
    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/zmq.toml")
        .configure()?;

    // Create `zmq` transport layer
    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    // Retrieve the general configuraiton of the bus from the `examples/bus.toml` configuration file
    let mut configuration =
        DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/bus.toml");

    // Create bus
    // When creating a bus, message handlers have to be registered. We this register the `handle_my_event` message handler.
    // Since `MyEvent` is routed, to be able to automatically subscribe at startup with the corresponding binding keys,
    // we must specify the binding key we want to subscribe to.
    // Here, the bus will subscribe to every `MyEvent` with a `key` matching the literal `"my-key"` string
    let bus = BusBuilder::new()
        .configure_with(PeerId::new("Quickstart.0"), "example", &mut configuration)?
        .with_handler(
            MessageHandler::with_state(()).handles(
                handle_my_event
                    .into_handler()
                    .bind(|binding| binding.key.matches("my-key".to_owned())),
            ),
        )
        .with_transport(zmq)
        .create()
        .await?;

    // Start the bus and log an error if it failed
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
