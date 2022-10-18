use thiserror::Error;

use super::ZmqSocketOptions;
use crate::PeerId;
use std::io::{self, Read};

/// Inbound socket error
#[derive(Clone, Copy, Eq, PartialEq, Debug, Error)]
pub enum Error {
    /// Zmq error
    #[error("zmq: {0}")]
    Zmq(zmq::Error),

    /// An operation was attempted while the socket was not in a valid state
    #[error("An operation was attempted while the socket was not in a valid state")]
    InvalidOperation,
}

pub(super) type Result<T> = std::result::Result<T, Error>;

enum Inner {
    Init {
        context: zmq::Context,
        peer_id: PeerId,
        endpoint: String,
        options: ZmqSocketOptions,
    },

    Bound {
        context: zmq::Context,
        peer_id: PeerId,
        endpoint: String,
        options: ZmqSocketOptions,
        socket: zmq::Socket,
    },
}

pub(super) struct ZmqInboundSocket {
    inner: Option<Inner>,
}

impl ZmqInboundSocket {
    /// Create a new [`ZmqInboundSocket`]
    pub(super) fn new(
        context: zmq::Context,
        peer_id: PeerId,
        endpoint: String,
        options: ZmqSocketOptions,
    ) -> Self {
        Self {
            inner: Some(Inner::Init {
                context,
                peer_id,
                endpoint,
                options,
            }),
        }
    }

    /// Bind the socket to the configured endpoint
    /// Return the endpoint that was bound by `zmq`
    pub(super) fn bind(&mut self) -> Result<String> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init {
                context,
                peer_id,
                endpoint,
                options,
            }) => {
                // Create socket
                let socket = Self::create_socket(&context, &options).map_err(Error::Zmq)?;

                // Bind the zmq socket
                socket.bind(&endpoint).map_err(Error::Zmq)?;

                // Get the final endpoint that was bound by zmq
                let bound_endpoint = socket
                    .get_last_endpoint()
                    .map_err(Error::Zmq)?
                    .expect("invalid UTF-8 returned by zmq");

                // Transition to Bound state
                (
                    Some(Inner::Bound {
                        context,
                        peer_id,
                        endpoint,
                        options,
                        socket,
                    }),
                    Ok(bound_endpoint),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    /// Undind and close the underlying `zmq` socket
    pub(super) fn close(&mut self) -> Result<()> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Bound {
                context,
                peer_id,
                endpoint,
                options,
                socket,
            }) => {
                // Drop the old socket
                std::mem::drop(socket);

                // Transition to Init state
                (
                    Some(Inner::Init {
                        context,
                        peer_id,
                        endpoint,
                        options,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn create_socket(context: &zmq::Context, opts: &ZmqSocketOptions) -> zmq::Result<zmq::Socket> {
        let socket = context.socket(zmq::SocketType::PULL)?;

        if let Some(recv_high_watermark) = opts.recv_high_watermark {
            socket.set_rcvhwm(recv_high_watermark)?;
        }

        if let Some(recv_timeout) = opts.recv_timeout {
            socket.set_rcvtimeo(recv_timeout.as_millis() as i32)?;
        }

        Ok(socket)
    }
}

impl Read for ZmqInboundSocket {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner {
            Some(Inner::Bound { ref socket, .. }) => Ok(socket
                .recv_into(buf, 0)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?),
            // TODO(oktal): Figure-out what we want to do with our error types to make them
            // implement `Error` trait
            _ => panic!("not handled yet"),
        }
    }
}
