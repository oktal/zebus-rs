use std::error::Error;
use std::time::Duration;
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{Bus, BusBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let zmq_configuration = ZmqTransportConfiguration {
        inbound_endpoint: "tcp://*:*".into(),
        wait_for_end_of_stream_ack_timeout: Duration::from_secs(10),
    };

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut bus = BusBuilder::new(zmq)
        .with_default_configuration("tcp://localhost:7465", "test".to_string())
        .create()
        .unwrap();

    if let Err(e) = bus.start() {
        eprintln!("{e}");
    }

    Ok(())
}
