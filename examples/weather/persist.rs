use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use tokio;
use zebus::configuration::DefaultConfigurationProvider;
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{inject, Bus, BusBuilder, BusConfiguration, ConfigurationProvider, Event, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone)]
struct WeatherStationState {
    measurements: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl WeatherStationState {
    fn new() -> Self {
        Self {
            measurements: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "Abc.Weather", routable)]
struct SensorTemperatureCollected {
    #[prost(string, required, tag = 1)]
    #[zebus(routing_position = 1)]
    pub name: String,

    #[prost(double, required, tag = 2)]
    pub measurement: f64,
}

async fn sensor_temperature_collected(
    ev: SensorTemperatureCollected,
    inject::State(state): inject::State<WeatherStationState>,
) {
    let SensorTemperatureCollected { name, measurement } = ev;
    info!("received measurement {measurement} for weather station {name}");

    state
        .measurements
        .lock()
        .expect("lock poisoned")
        .entry(name)
        .or_insert(Vec::new())
        .push(measurement);
}

use tracing::{error, info};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/zmq.toml")
        .configure()?;

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut configuration =
        DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/bus.toml");

    let routing = std::env::args().skip(1).next();
    let instance_id = if let Some(routing) = routing.as_ref() {
        info!("Subscribing to {routing} station measurements");
        routing.to_uppercase().replace(" ", "_")
    } else {
        info!("Subscribing to all station measurements");
        "ALL".to_string()
    };

    let bus = BusBuilder::new()
        .configure_with(
            PeerId::new(format!("Abc.Weather.Persist.{instance_id}")),
            "example",
            &mut configuration,
        )?
        .handles(
            MessageHandler::with_state(WeatherStationState::new()).handles(
                sensor_temperature_collected.into_handler().bind(|binding| {
                    if let Some(routing) = routing {
                        binding.name.matches(routing)
                    } else {
                        binding.name.any()
                    }
                }),
            ),
        )
        .with_transport(zmq)
        .create()
        .await?;

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
