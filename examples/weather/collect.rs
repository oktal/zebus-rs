use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{self, BufRead};
use std::num::ParseFloatError;
use std::str::FromStr;
use tokio;
use tracing::{debug, error};
use zebus::configuration::DefaultConfigurationProvider;
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder, BusConfiguration, ConfigurationProvider, Event, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "Abc.Weather", routable)]
struct SensorTemperatureCollected {
    #[prost(string, required, tag = 1)]
    #[zebus(routing_position = 1)]
    pub name: String,

    #[prost(double, required, tag = 2)]
    pub measurement: f64,
}

#[derive(Debug)]
enum ParseMeasurementError {
    InvalidLine(String),

    InvalidMeasurement(ParseFloatError, String),
}

impl Display for ParseMeasurementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLine(s) => write!(f, "invalid measurement line {s}"),
            Self::InvalidMeasurement(err, measurement) => {
                write!(f, "invalid measurement {measurement}: {err}")
            }
        }
    }
}

impl std::error::Error for ParseMeasurementError {}

impl FromStr for SensorTemperatureCollected {
    type Err = ParseMeasurementError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, measurement) = s
            .split_once(";")
            .ok_or(ParseMeasurementError::InvalidLine(s.to_string()))?;

        let measurement = measurement
            .parse()
            .map_err(|e| ParseMeasurementError::InvalidMeasurement(e, measurement.to_string()))?;

        let name = name.to_string();

        Ok(Self { name, measurement })
    }
}

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

    let bus = BusBuilder::new()
        .configure_with(
            PeerId::new("Abc.Weather.Collect.0"),
            "example",
            &mut configuration,
        )?
        .with_transport(zmq)
        .create()
        .await?;

    if let Err(e) = bus.start().await {
        error!("failed to start bus: {e}");
    } else {
        let measurements = std::env::args()
            .skip(1)
            .next()
            .unwrap_or("examples/weather/measurements.txt".to_string());

        let file = File::open(measurements)?;
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let measurement = line?.parse::<SensorTemperatureCollected>()?;

            debug!("publishing {measurement:?}");
            bus.publish(&measurement).await?;

            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        bus.stop().await.expect("Failed to stop bus");
    }

    Ok(())
}
