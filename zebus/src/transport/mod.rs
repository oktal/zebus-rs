//! Transport base layer for Zebus peer-to-peer communication
mod message_execution_completed;
mod originator_info;
mod send_context;
mod transport_message;
pub mod zmq;

use crate::{directory::event::PeerEvent, Peer, PeerId};
use futures_core::{stream::BoxStream, Stream};
use std::{borrow::Cow, sync::Arc};

pub use message_execution_completed::MessageExecutionCompleted;
pub use originator_info::OriginatorInfo;
pub use send_context::SendContext;
use tokio::runtime::Runtime;
pub use transport_message::TransportMessage;

/// Transport layer trait
pub trait Transport: Send + 'static {
    /// The associated error type which can be returned from the transport layer
    type Err: std::error::Error + 'static;

    /// Type of [`TransportMessage`] [`Stream`]
    /// The stream is used to receive messages from the transport
    type MessageStream: Stream<Item = TransportMessage> + Unpin + Send + 'static;

    /// Configure this transport layer with a [`PeerId`]
    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory_rx: BoxStream<'static, PeerEvent>,
        runtime: Arc<Runtime>,
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
