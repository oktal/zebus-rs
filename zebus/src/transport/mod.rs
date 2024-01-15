//! Transport base layer for Zebus peer-to-peer communication
mod message_execution_completed;
mod originator_info;
mod send_context;
mod transport_message;
pub mod zmq;

use crate::{directory, BoxError, Message, Peer, PeerId};
use futures_core::Stream;
use std::borrow::Cow;

pub use message_execution_completed::MessageExecutionCompleted;
pub use originator_info::OriginatorInfo;
pub use send_context::SendContext;
pub use transport_message::TransportMessage;

/// Transport layer trait
pub trait Transport: Send + Sync + 'static {
    /// The associated error type which can be returned from the transport layer
    /// The error type must be convertible to a [`BoxError`]
    type Err: Into<BoxError>;

    /// Type of [`TransportMessage`] [`Stream`]
    /// The stream is used to receive messages from the transport
    type MessageStream: Stream<Item = TransportMessage> + Unpin + Send + 'static;

    /// Configure this transport layer with a [`PeerId`]
    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory_rx: directory::EventStream,
    ) -> Result<(), Self::Err>;

    /// Create a new subscription to the transport messages stream
    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err>;

    /// Start the transport layer
    fn start(&mut self) -> Result<(), Self::Err>;

    /// Stop the transport layer
    fn stop(&mut self) -> Result<(), Self::Err>;

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
    ) -> Result<(), Self::Err>;
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
    fn send_one(
        &mut self,
        peer: Peer,
        message: &dyn Message,
        context: SendContext,
    ) -> Result<uuid::Uuid, <Self as Transport>::Err> {
        let self_peer = self.peer()?;
        let environment = self.environment()?.into_owned();

        let (message_id, message) = TransportMessage::create(&self_peer, environment, message);

        self.send(std::iter::once(peer), message, context)?;

        Ok(message_id)
    }
}

impl<T> TransportExt for T where T: Transport {}
