use std::{
    collections::HashMap,
    io,
    sync::{self, mpsc},
    thread::JoinHandle,
};

use super::outbound::ZmqOutboundSocket;
use crate::{
    transport::{
        zmq::{ZmqSocketOptions, ZmqTransportConfiguration},
        SendContext, Transport, TransportMessage,
    },
    Peer, PeerId,
};

const OUTBOUND_THREAD_NAME: &'static str = "outbound";

/// Associated Error type with zmq
pub enum Error {
    /// Error from zmq
    Zmq(zmq::Error),

    /// IO Error
    Io(io::Error),

    /// An operation was attempted while the [`ZmqTransport`] was in an invalid state for the
    /// operation
    InvalidOperation,
}

enum OuboundSocketAction {
    Send {
        message: TransportMessage,

        peers: Vec<Peer>,

        context: SendContext,
    },

    Disconnect {
        peer_id: PeerId,
    },
}

enum Inner {
    Unconfigured {
        configuration: ZmqTransportConfiguration,
        options: ZmqSocketOptions,
    },
    Configured {
        configuration: ZmqTransportConfiguration,

        options: ZmqSocketOptions,

        peer_id: PeerId,

        environment: String,
    },
    Started {
        configuration: ZmqTransportConfiguration,

        options: ZmqSocketOptions,

        peer_id: PeerId,

        environment: String,

        context: zmq::Context,

        outbound_thread: JoinHandle<()>,

        outbound_socket_action_tx: sync::mpsc::Sender<OuboundSocketAction>,

        outbound_sockets: HashMap<PeerId, ZmqOutboundSocket>,
    },
}

/// Zmq-based [`Transport`] implementation
pub struct ZmqTransport {
    // Invariant: the underlying [`Option`] is always `Some`.
    // We use the `Option` as a way to move the inner state from a mutable
    // reference when transitioning between states
    inner: Option<Inner>,
}

impl ZmqTransport {
    pub fn new(configuration: ZmqTransportConfiguration, options: ZmqSocketOptions) -> Self {
        Self {
            inner: Some(Inner::Unconfigured {
                configuration,
                options,
            }),
        }
    }
}

fn outbound_worker(rx: mpsc::Receiver<OuboundSocketAction>) {}

impl Transport for ZmqTransport {
    type Err = Error;

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Unconfigured {
                configuration,
                options,
            }) => (
                // Transition to Configured state
                Some(Inner::Configured {
                    configuration,
                    options,
                    peer_id,
                    environment,
                }),
                Ok(()),
            ),
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn start(&mut self) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                options,
                peer_id,
                environment,
            }) => {
                // Create outbound channel for outbound socket operations
                let (outbound_socket_action_tx, outbound_socket_action_rx) = mpsc::channel();

                // Create zmq context
                let context = zmq::Context::new();

                // Spawn outbound thread
                let outbound_thread = std::thread::Builder::new()
                    .name(OUTBOUND_THREAD_NAME.into())
                    .spawn(move || {
                        outbound_worker(outbound_socket_action_rx);
                    })
                    .map_err(Error::Io)?;

                (
                    // Transition to Started state
                    Some(Inner::Started {
                        configuration,
                        options,
                        peer_id,
                        environment,
                        context,
                        outbound_thread,
                        outbound_socket_action_tx,
                        outbound_sockets: HashMap::new(),
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<(), Self::Err> {
        todo!()
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn inbound_endpoint(&self) -> Result<String, Self::Err> {
        todo!()
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<(), Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref outbound_socket_action_tx,
                ..
            }) => {
                let peers = peers.collect();

                // TODO(oktal): Can `send` fail ?
                outbound_socket_action_tx
                    .send(OuboundSocketAction::Send {
                        message,
                        peers,
                        context,
                    })
                    .expect("unexpected send failure");

                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }
}
