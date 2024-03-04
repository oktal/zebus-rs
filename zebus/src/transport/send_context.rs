use crate::{Peer, PeerId};

/// Represents the context associated with message sending
/// This struct is used to encapsulate useful information
/// for the [`crate::Transport`] layer when sending a message
/// to a list of recipient peers
#[derive(Debug)]
pub struct SendContext {
    /// List of target peers that are persistent
    pub(crate) persistent_peer_ids: Vec<PeerId>,

    /// [`Peer`] of the persistence service
    /// When [`None`], the persistence service peer is unknown
    pub(crate) persistence_peer: Option<Peer>,
}

impl SendContext {
    pub(crate) fn was_persisted(&self, peer_id: &PeerId) -> bool {
        self.persistent_peer_ids
            .iter()
            .find(|&p| p == peer_id)
            .is_some()
    }
}

impl Default for SendContext {
    fn default() -> Self {
        Self {
            persistent_peer_ids: vec![],
            persistence_peer: None,
        }
    }
}
