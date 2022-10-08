use zmq::Context;

use crate::{transport::zmq::ZmqSocketOptions, PeerId};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum State {
    Closed,
    Opened,
    Connected,
}

pub(crate) struct ZmqOutboundSocket {
    /// Underlying zmq socket
    socket: zmq::Socket,

    /// Peer Id
    peer_id: PeerId,

    /// Endpoint to connect
    endpoint: Option<String>,

    /// State of the underlying connection
    state: State,
}

impl ZmqOutboundSocket {
    /// Create a new zmq outbound socket from a zmq [`Context`] and an associated [`PeerId`] and default [`ZmqSocketOptions`]
    pub(crate) fn new(context: &Context, peer_id: PeerId) -> zmq::Result<Self> {
        Self::new_with_options(context, peer_id, ZmqSocketOptions::default())
    }

    /// Create a new zmq outbound socket from a zmq [`Context`] with an associated [`PeerId`] and [`ZmqSocketOptions`]
    pub(crate) fn new_with_options(
        context: &Context,
        peer_id: PeerId,
        opts: ZmqSocketOptions,
    ) -> zmq::Result<Self> {
        Ok(Self {
            socket: Self::create_socket(context, opts)?,
            peer_id,
            endpoint: None,
            state: State::Opened,
        })
    }

    /// Connect the underlying zmq socket to the specified `endpoint`
    pub(crate) fn connect(&mut self, endpoint: &str) -> zmq::Result<()> {
        self.socket.connect(endpoint)?;
        self.endpoint = Some(endpoint.to_string());
        self.state = State::Connected;
        Ok(())
    }

    /// Get the endpoint associated with the connection.
    /// Returns [`None`] if the socket is not connected
    pub(crate) fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Returns `true` if the underlying socket is connected
    pub(crate) fn is_connected(&self) -> bool {
        self.state == State::Connected
    }

    fn create_socket(context: &Context, opts: ZmqSocketOptions) -> zmq::Result<zmq::Socket> {
        let socket = context.socket(zmq::SocketType::PUSH)?;

        if let Some(send_high_watermark) = opts.send_high_watermark {
            socket.set_sndhwm(send_high_watermark)?;
        }

        if let Some(send_timeout) = opts.send_timeout {
            socket.set_sndtimeo(send_timeout.as_millis() as i32)?;
        }

        if let Some(keep_alive_opts) = opts.keep_alive {
            socket.set_tcp_keepalive(1)?;

            if let Some(keep_alive_timeout) = keep_alive_opts.timeout {
                socket.set_tcp_keepalive_idle(keep_alive_timeout.as_secs() as i32)?;
            }

            if let Some(keep_alive_interval) = keep_alive_opts.interval {
                socket.set_tcp_keepalive_intvl(keep_alive_interval.as_secs() as i32)?;
            }
        } else {
            socket.set_tcp_keepalive(0)?;
        }

        Ok(socket)
    }
}
