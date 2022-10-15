use crate::{Peer, PeerId};

pub struct SendContext {
    pub persistent_peer_ids: Vec<PeerId>,

    pub persistent_peer: Option<Peer>,
}

impl SendContext {
    pub fn was_persisted(&self, peer_id: &PeerId) -> bool {
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
            persistent_peer: None,
        }
    }
}
