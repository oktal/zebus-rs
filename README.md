# Zebus

Zebus is a peer-to-peer serivce bus communication layer to build micro services oriented architecture in Rust.
It allows peers to send commands to each other and publish events to multiple peers in a decentralized network.

Zebus abstracts away the complexities of transport and protocol layers, providing a simple and unified interface for communication
between peers through a `Bus` interface.

```rs
use std::error::Error;
use std::io::{self, Read};
use tokio;

use zebus::configuration::DefaultConfigurationProvider;
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder, BusConfiguration, Command, ConfigurationProvider, Event, PeerId};

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Echo")]
pub struct EchoCommand {
    #[prost(string, required, tag = 1)]
    msg: String,
}

async fn echo(cmd: EchoCommand) {
    println!("ECHO {}", cmd.msg);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/zmq.toml")
        .configure()?;

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut configuration =
        DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/bus.toml");

    let bus = BusBuilder::new()
        .configure_with(PeerId::new("Echo.0"), "example", &mut configuration)?
        .handles(MessageHandler::with_state(()).handles(echo.into_handler()))
        .with_transport(zmq)
        .create()
        .await?;

    if let Err(e) = bus.start().await {
        eprintln!("failed to start bus: {e}");
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
```

## Features

- **Peer-to-Peer Communication**: Zebus facilitates direct communication between peers in a decentralized network, eliminating the need for a centralized message broker.
- **Command Transmission**: Peers can send commands to each other to perform an action.
- **Event Publishing**: Peers can publish events to notify that an action has been performed.
- **Routing Support**: Zebus supports both direct and routed messaging, enabling efficient delivery of messages to specific peers.
- **Decentralized**: Zebus operates in a decentralized manner, allowing for resilient and scalable communication.

## Concepts

### Peer

A Peer in Zebus represents a node or participant connected to the bus.
Each peer is capable of sending commands, publishing events, and handling incoming messages from other peers.
Peers interact with each other to exchange information and coordinate actions.
Each peer in Zebus typically encapsulates application-specific logic and functionality, allowing for modular and distributed system architectures.

### Commands

Commands in Zebus represent actions that a peer intends to perform on another peer.
Each command is directed towards a single recipient peer and can carry data or instructions necessary for the intended action.
Commands enable peers to interact with each other in a request-response manner, facilitating various operations within the bus.

### Events

Events in Zebus are messages that peers publish to notify other peers about specific occurrences or changes in state.
Unlike commands, events are not targeted towards a single recipient but rather broadcasted to multiple peers in the bus.
Peers interested in specific types of events can subscribe to receive them, enabling real-time communication and synchronization among decentralized components.

### Routing

Routing in Zebus refers to the mechanism by which commands and events are directed through the bus to reach their intended recipients.
Zebus provides support for both direct and routed messaging, allowing peers to communicate efficiently in various network topologies.

### Handlers

Handlers in Zebus are functions or methods implemented by peers to process incoming commands or events.
When a peer receives a command, it invokes the corresponding command handler to execute the requested action.
Similarly, when an event is published, Zebus invokes the appropriate event handlers registered by interested peers to handle the event.
Handlers allow peers to react to incoming messages and perform necessary actions, enabling seamless communication and cooperation within the bus.
