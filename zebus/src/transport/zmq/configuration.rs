use std::time::Duration;

/// Configuration parameters for a zmq [`super::ZmqTransport`] transport
#[derive(Clone, Debug)]
pub struct ZmqTransportConfiguration {
    /// The endpoint to bind to (e.g tcp://hostname:port, tcp://*.*, ...) used for communication
    /// with other peers
    pub inbound_endpoint: String,

    pub wait_for_end_of_stream_ack_timeout: Duration,
}
