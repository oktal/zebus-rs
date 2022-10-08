//! Transport base layer for Zebus peer-to-peer communication
mod originator_info;
mod send_context;
mod transport_message;
pub mod zmq;
use std::borrow::Cow;

use crate::{Peer, PeerId};

pub use originator_info::OriginatorInfo;
pub use send_context::SendContext;
pub use transport_message::TransportMessage;

/// Transport layer trait
pub trait Transport {
    /// The associated error type which can be returned from the transport layer
    type Err;

    /// Configure this transport layer with a [`PeerId`]
    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err>;

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

    /// Send a `message` to a list of `peers`
    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Result<(), Self::Err>;
}
