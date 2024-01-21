use std::borrow::Cow;

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::BusEvent,
    sync::stream::EventStream,
    transport::{self, future::SendFuture, Transport, TransportMessage},
    BusConfiguration, PeerId,
};

use super::{PersistenceError, PersistenceRequest};

enum Inner<T> {
    Init {
        /// Configuration of the bus
        configuration: BusConfiguration,

        /// Inner transport
        inner: T,
    },

    Configured {
        /// Configuration of the bus
        configuration: BusConfiguration,

        /// [`PeerId`] that has ben configured
        peer_id: PeerId,

        /// Environment that has been configured
        environment: String,

        /// Inner transport
        inner: T,

        /// Bus event reception stream
        event_rx: EventStream<BusEvent>,
    },

    Started {
        /// Configuration of the bus
        _configuration: BusConfiguration,

        /// [`PeerId`] that has ben configured
        peer_id: PeerId,

        /// Environment that has been configured
        environment: String,

        /// The endpoint that has been bound by the inner transport
        inbound_endpoint: String,

        /// Channel to receive [`TransportMessage`] messages from
        /// Use to subscribe
        messages_tx: broadcast::Sender<TransportMessage>,

        /// Channel to send requests to persist messages
        requests_tx: tokio::sync::mpsc::Sender<PersistenceRequest>,

        /// Token to cancel task
        shutdown: CancellationToken,

        /// Future that resolves when the task has been joined
        stop: super::future::StopFuture,
    },
}

pub struct PersistentTransport<T> {
    inner: Option<Inner<T>>,
}

impl<T> PersistentTransport<T>
where
    T: Transport,
{
    pub(crate) fn new(configuration: BusConfiguration, inner: T) -> Self {
        Self {
            inner: Some(Inner::Init {
                configuration,
                inner,
            }),
        }
    }
}

impl<T> Transport for PersistentTransport<T>
where
    T: Transport,
{
    type Err = PersistenceError;
    type MessageStream = super::MessageStream;

    type StartCompletionFuture = super::future::StartFuture;
    type StopCompletionFuture = super::future::StopFuture;
    type SendFuture = transport::future::SendFuture<PersistenceRequest, Self::Err>;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        event: EventStream<BusEvent>,
    ) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init {
                configuration,
                mut inner,
            }) => {
                // Configure the inner transport
                inner
                    .configure(peer_id.clone(), environment.clone(), event.clone())
                    .map_err(Into::into)?;

                // Transition to Configured state
                (
                    Some(Inner::Configured {
                        configuration,
                        peer_id,
                        environment,
                        inner,
                        event_rx: event,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(PersistenceError::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref messages_tx, ..
            }) => Ok(messages_tx.subscribe().into()),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn start(&mut self) -> Result<Self::StartCompletionFuture, Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                peer_id,
                environment,
                mut inner,
                event_rx,
            }) => {
                // Start the underlying transport
                inner.start().map_err(Into::into)?;

                // Retrieve bound endpoint
                let inbound_endpoint = inner.inbound_endpoint().map_err(Into::into)?.into_owned();

                // Create broadcast channel for transport message reception
                let (messages_tx, _messages_rx) = broadcast::channel(128);

                let shutdown = CancellationToken::new();

                let (requests_tx, events_rx, stop) = super::service::spawn(
                    &configuration,
                    event_rx.stream(),
                    inner,
                    messages_tx.clone(),
                    shutdown.clone(),
                );

                let start = super::future::StartFuture::spawn(events_rx, &configuration);

                // Transition to Started state
                (
                    Some(Inner::Started {
                        _configuration: configuration,
                        peer_id,
                        environment,
                        inbound_endpoint,
                        messages_tx,
                        requests_tx,
                        shutdown,
                        stop,
                    }),
                    Ok(start),
                )
            }
            x => (x, Err(PersistenceError::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<Self::StopCompletionFuture, Self::Err> {
        match self.inner.take() {
            Some(Inner::Started { shutdown, stop, .. }) => {
                // Cancel the task
                shutdown.cancel();

                // TODO(oktal): we should go back to the `Configured` state but we need to retrieve
                // the inner transport layer that was moved to an inner task

                Ok(stop)
            }
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn environment(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured {
                ref environment, ..
            })
            | Some(Inner::Started {
                ref environment, ..
            }) => Ok(Cow::Borrowed(environment.as_str())),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn inbound_endpoint(&self) -> Result<std::borrow::Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref inbound_endpoint,
                ..
            }) => Ok(Cow::Borrowed(inbound_endpoint.as_str())),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = crate::Peer>,
        message: TransportMessage,
        _context: crate::transport::SendContext,
    ) -> Result<Self::SendFuture, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { requests_tx, .. }) => {
                Ok(SendFuture::new(requests_tx.clone(), peers, message))
            }
            _ => Err(PersistenceError::InvalidOperation),
        }
    }
}
