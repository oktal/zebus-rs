//! Transport base layer for Zebus peer-to-peer communication
pub(crate) mod future;
#[cfg(test)]
pub(crate) mod memory;
mod message_execution_completed;
mod originator_info;
mod send_context;
mod transport_message;
pub mod zmq;

use crate::{
    bus::BusEvent, directory::DirectoryReader, persistence::transport::PersistentTransport,
    sync::stream::EventStream, BoxError, BusConfiguration, Message, Peer, PeerId,
};
use futures_core::{future::BoxFuture, Future, Stream};
use futures_util::FutureExt;
use std::{borrow::Cow, sync::Arc};

pub use message_execution_completed::MessageExecutionCompleted;
pub use originator_info::OriginatorInfo;
pub use send_context::SendContext;
pub use transport_message::TransportMessage;

/// Transport layer trait
pub trait Transport: Send + Sync + 'static {
    /// The associated error type which can be returned from the transport layer
    /// The error type must be convertible to a [`BoxError`]
    type Err: Into<BoxError> + std::error::Error + Send;

    /// Type of [`TransportMessage`] [`Stream`]
    /// The stream is used to receive messages from the transport
    type MessageStream: Stream<Item = TransportMessage> + Unpin + Send + 'static;

    /// A [`Future`] type returned by `start` that will resolve when the start operation completes
    /// Note that the future does not need to be poll-ed or await-ed for the operation to make
    /// progress.
    /// That start operation should start immediately and return a [`Future`] type that can
    /// be await-ed to wait for the start to complete
    type StartCompletionFuture: Future<Output = Result<(), Self::Err>> + Send + 'static;

    /// A [`Future`] type returned by `stop` that will resolve when the stop operation completes
    /// Note that the future does not need to be poll-ed or await-ed for the operation to make
    /// progress.
    /// That stop operation should start immediately and return a [`Future`] type that can
    /// be await-ed to wait for the stop to complete
    type StopCompletionFuture: Future<Output = Result<(), Self::Err>> + Send + 'static;

    /// A [`Future`] type returned by `send`
    /// Note that this future must be polled-ed or await-ed for the operation to make progress
    type SendFuture: Future<Output = Result<(), Self::Err>> + Send + 'static;

    /// Configure this transport layer with a [`PeerId`]
    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory: Arc<dyn DirectoryReader>,
        event: EventStream<BusEvent>,
    ) -> Result<(), Self::Err>;

    /// Create a new subscription to the transport messages stream
    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err>;

    /// Start the transport layer
    fn start(&mut self) -> Result<Self::StartCompletionFuture, Self::Err>;

    /// Stop the transport layer
    fn stop(&mut self) -> Result<Self::StopCompletionFuture, Self::Err>;

    /// Get the [`PeerId`] associated with the transport layer.
    /// This can fail if the transport layer has not been configured properly
    fn peer_id(&self) -> Result<&PeerId, Self::Err>;

    /// Retrieve the environment associated with this transport layer
    /// This can fail if the transport layer has not been configured properly
    fn environment(&self) -> Result<Cow<'_, str>, Self::Err>;

    /// Get the associated endpoint that has been bound by the transport layer
    /// This can fail if the transport layer has not been configured properly
    fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err>;

    /// Send a [`TransportMessage`] to a list of peers
    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<Self::SendFuture, Self::Err>;
}

/// Extension methods for [`Transport`]
pub trait TransportExt: Transport {
    /// Retrieve the [`Peer`] associated with this transport layer
    /// This can fail if the transport layer has not been configured properly
    fn peer(&self) -> Result<Peer, <Self as Transport>::Err> {
        Ok(Peer {
            id: self.peer_id()?.clone(),
            endpoint: self.inbound_endpoint()?.into_owned(),
            is_up: true,
            is_responding: true,
        })
    }

    /// Send one [`Message`] to a destination [`Peer`]
    fn send_one<'a>(
        &'a mut self,
        peer: Peer,
        message: &dyn Message,
        context: SendContext,
    ) -> BoxFuture<Result<uuid::Uuid, Self::Err>> {
        match self.create_message(message) {
            Ok((id, msg)) => Box::pin(async move {
                self.send(std::iter::once(peer), msg, context)?.await?;
                Ok(id)
            }),
            Err(e) => futures_util::future::ready(Err(e)).boxed(),
        }
    }

    fn create_message(
        &mut self,
        message: &dyn Message,
    ) -> Result<(uuid::Uuid, TransportMessage), <Self as Transport>::Err> {
        let self_peer = self.peer()?;
        let environment = self.environment()?.into_owned();

        Ok(TransportMessage::create(&self_peer, environment, message))
    }

    fn persistent(self, configuration: BusConfiguration) -> PersistentTransport<Self>
    where
        Self: Sized,
    {
        PersistentTransport::new(configuration, self)
    }
}

impl<T> TransportExt for T where T: Transport {}
