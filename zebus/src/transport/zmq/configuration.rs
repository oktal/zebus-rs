use std::time::Duration;

#[cfg(feature = "config-provider")]
use serde::{Deserialize, Serialize};

/// Configuration parameters for a zmq [`super::ZmqTransport`] transport
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "config-provider", derive(Serialize, Deserialize))]
pub struct ZmqTransportConfiguration {
    /// The endpoint to bind to (e.g tcp://hostname:port, tcp://*.*, ...) used for communication
    /// with other peers
    pub inbound_endpoint: String,

    #[cfg_attr(feature = "config-provider", serde(with = "humantime_serde"))]
    pub wait_for_end_of_stream_ack_timeout: Duration,
}
