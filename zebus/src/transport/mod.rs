//! Transport base layer for Zebus peer-to-peer communication
mod message_execution_completed;
mod originator_info;
mod send_context;
mod transport_message;
pub mod zmq;

use crate::{directory, BoxError, Message, MessageId, Peer, PeerId};
use futures_core::{future::BoxFuture, Future, Stream};
use futures_util::FutureExt;
use std::borrow::Cow;

pub use message_execution_completed::MessageExecutionCompleted;
pub use originator_info::OriginatorInfo;
pub use send_context::SendContext;
pub use transport_message::TransportMessage;

/// Transport layer trait
pub trait Transport: Send + Sync + 'static {
    /// The associated error type which can be returned from the transport layer
    /// The error type must be convertible to a [`BoxError`]
    type Err: Into<BoxError> + Send;

    /// Type of [`TransportMessage`] [`Stream`]
    /// The stream is used to receive messages from the transport
    type MessageStream: Stream<Item = TransportMessage> + Unpin + Send + 'static;

    type Future: Future<Output = Result<(), Self::Err>> + Send;

    /// Configure this transport layer with a [`PeerId`]
    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory_rx: directory::EventStream,
    ) -> Self::Future;

    /// Create a new subscription to the transport messages stream
    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err>;

    /// Start the transport layer
    fn start(&mut self) -> Self::Future;

    /// Stop the transport layer
    fn stop(&mut self) -> Self::Future;

    /// Get the [`PeerId`] associated with the transport layer.
    /// This can fail if the transport layer has not been configured properly
    fn peer_id(&self) -> Result<&PeerId, Self::Err>;
    ///
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
    ) -> Self::Future;
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
    // NOTE(oktal): we could avoid boxing the future by creating our own
    // Future response type but this extension function should not be
    // used often enough to justify the need for it
    fn send_one<'a>(
        &'a mut self,
        peer: Peer,
        message: &dyn Message,
        context: SendContext,
    ) -> futures_util::future::Either<
        futures_util::future::Ready<Result<uuid::Uuid, <Self as Transport>::Err>>,
        BoxFuture<'a, Result<uuid::Uuid, <Self as Transport>::Err>>,
    > {
        match self.create_message(message) {
            Ok((id, msg)) => async move {
                self.send(std::iter::once(peer), msg, context).await?;
                Ok(id)
            }
            .boxed()
            .right_future(),
            Err(e) => futures_util::future::ready(Err(e)).left_future(),
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
}

impl<T> TransportExt for T where T: Transport {}
