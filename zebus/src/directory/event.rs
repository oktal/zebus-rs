use crate::PeerId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    /// A new peer [`PeerId`] has been started
    Started(PeerId),

    /// Peer [`PeerId`] has been stopped
    Stopped(PeerId),

    /// Peer [`PeerId`] has been updated
    Updated(PeerId),

    /// Peer [`PeerId`] has been decomissionned
    Decomissionned(PeerId),
}

impl PeerEvent {
    pub(crate) fn peer_id(&self) -> &PeerId {
        match self {
            PeerEvent::Started(peer_id)
            | PeerEvent::Stopped(peer_id)
            | PeerEvent::Updated(peer_id)
            | PeerEvent::Decomissionned(peer_id) => peer_id,
        }
    }
}
