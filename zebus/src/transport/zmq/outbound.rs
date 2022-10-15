use std::io::{self, Write};

use zmq::Context;

use crate::{transport::zmq::ZmqSocketOptions, PeerId};

/// Outbound socket error
pub enum Error {
    /// Zmq error
    Zmq(zmq::Error),

    /// An operation was attempted while the socket was not in a valid state
    InvalidOperation,
}

pub(super) type Result<T> = std::result::Result<T, Error>;

enum Inner {
    Init {
        context: zmq::Context,
        peer_id: PeerId,
        options: ZmqSocketOptions,
    },
    Connected {
        socket: zmq::Socket,
        endpoint: String,
        peer_id: PeerId,
    },
}

pub(super) struct ZmqOutboundSocket {
    inner: Option<Inner>,
}

impl ZmqOutboundSocket {
    /// Create a new zmq outbound socket from a zmq [`Context`] and an associated [`PeerId`] and default [`ZmqSocketOptions`]
    pub(super) fn new(context: Context, peer_id: PeerId) -> Self {
        Self::new_with_options(context, peer_id, ZmqSocketOptions::default())
    }

    /// Create a new zmq outbound socket from a zmq [`Context`] with an associated [`PeerId`] and [`ZmqSocketOptions`]
    pub(super) fn new_with_options(
        context: Context,
        peer_id: PeerId,
        options: ZmqSocketOptions,
    ) -> Self {
        Self {
            inner: Some(Inner::Init {
                context,
                peer_id,
                options,
            }),
        }
    }

    /// Connect the underlying zmq socket to the specified `endpoint`
    pub(super) fn connect(&mut self, endpoint: &str) -> Result<()> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init {
                context,
                peer_id,
                options,
            }) => {
                // Create the zmq socket
                let socket = Self::create_socket(&context, options).map_err(Error::Zmq)?;

                // Connect the socket
                socket.connect(endpoint).map_err(Error::Zmq)?;

                // Transition to connected state
                (
                    Some(Inner::Connected {
                        socket,
                        endpoint: endpoint.to_string(),
                        peer_id,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    /// Get the endpoint associated with the connection.
    /// Returns [`Error`] `InvalidOperation` if the socket is not connected
    pub(super) fn endpoint(&self) -> Result<&str> {
        match self.inner.as_ref() {
            Some(Inner::Connected { ref endpoint, .. }) => Ok(endpoint),
            _ => Err(Error::InvalidOperation),
        }
    }

    /// Returns `true` if the underlying socket is connected
    pub(super) fn is_connected(&self) -> bool {
        matches!(self.inner, Some(Inner::Connected { .. }))
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

impl Write for ZmqOutboundSocket {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.inner.as_ref() {
            Some(Inner::Connected { ref socket, .. }) => socket
                .send(buf, 0)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .map(|()| buf.len()),
            // TODO(oktal): Figure-out what we want to do with our error types to make them
            // implement `Error` trait
            _ => panic!("not handled yet"),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // There is no explicit command for flushing a specific message or all messages from the message queue.
        Ok(())
    }
}
