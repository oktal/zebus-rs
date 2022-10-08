use crate::{Peer, PeerId};

pub struct SendContext {
    pub persistent_peer_ids: Vec<PeerId>,

    pub persistent_peer: Option<Peer>,
}

impl Default for SendContext {
    fn default() -> Self {
        Self {
            persistent_peer_ids: vec![],
            persistent_peer: None,
        }
    }
}
