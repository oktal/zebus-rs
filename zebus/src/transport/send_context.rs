use crate::{Peer, PeerId};

/// Represents the context associated with message sending
/// This struct is used to encapsulate useful information
/// for the [`crate::Transport`] layer when sending a message
/// to a list of recipient peers
#[derive(Debug)]
pub enum SendContext {
    /// Used to instruct that a message is persistent and should thus be sent to the persistence service peer
    Persistent {
        /// List of target peers that are persistent
        persistent_peer_ids: Vec<PeerId>,

        /// [`Peer`] of the persistence service
        persistence_peer: Peer,
    },

    /// No context
    Empty,
}

impl Default for SendContext {
    fn default() -> Self {
        Self::Empty
    }
}
