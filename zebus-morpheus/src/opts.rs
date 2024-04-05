use std::time::Duration;

use clap::Parser;
use zebus::{
    configuration::{
        DEFAULT_MAX_BATCH_SIZE, DEFAULT_REGISTRATION_TIMEOUT, DEFAULT_START_REPLAY_TIMEOUT,
    },
    dispatch::InvokerService,
    transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration},
    Bus, BusBuilder, BusConfiguration, PeerId,
};

#[derive(Debug, Parser, Clone)]
#[command(version, about, long_about = None)]
pub struct Opts {
    /// Bus environment
    #[clap(long)]
    pub environment: String,

    /// List of directory endpoints to register to
    #[clap(long)]
    pub directory_endpoints: Vec<String>,
}

impl Opts {
    pub async fn create_bus<H>(
        &self,
        peer_id: impl Into<PeerId>,
        inbound_endpoint: Option<String>,
        handler: H,
    ) -> anyhow::Result<impl Bus>
    where
        H: InvokerService + 'static,
    {
        let zmq = ZmqTransport::new(
            ZmqTransportConfiguration {
                inbound_endpoint: inbound_endpoint.unwrap_or("tcp://*:*".into()),
                wait_for_end_of_stream_ack_timeout: Duration::from_secs(30),
            },
            ZmqSocketOptions::default(),
        );

        let configuration = BusConfiguration {
            directory_endpoints: self.directory_endpoints.clone(),
            registration_timeout: DEFAULT_REGISTRATION_TIMEOUT,
            start_replay_timeout: DEFAULT_START_REPLAY_TIMEOUT,
            is_persistent: false,
            pick_random_directory: true,
            enable_error_publication: false,
            message_batch_size: DEFAULT_MAX_BATCH_SIZE,
        };

        let bus = BusBuilder::new()
            .configure(peer_id, self.environment.clone(), configuration)
            .with_handler(handler)
            .with_transport(zmq)
            .create()
            .await?;

        Ok(bus)
    }
}
