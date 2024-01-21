use crate::{Peer, PeerId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEventKind {
    Started,
    Stopped,
    Updated,
    Decomissionned,
}

impl PeerEventKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            PeerEventKind::Started => "PeerStarted",
            PeerEventKind::Stopped => "PeerStopped",
            PeerEventKind::Updated => "PeerUpdated",
            PeerEventKind::Decomissionned => "PeerDecomissioned",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    /// A new peer as been started
    Started(Peer),

    /// Peer been stopped
    Stopped(Peer),

    /// Peer has been updated
    Updated(Peer),

    /// Peer has been decomissionned
    Decomissionned(Peer),
}

impl PeerEvent {
    pub(crate) fn peer_id(&self) -> &PeerId {
        match self {
            PeerEvent::Started(peer)
            | PeerEvent::Stopped(peer)
            | PeerEvent::Updated(peer)
            | PeerEvent::Decomissionned(peer) => &peer.id,
        }
    }

    pub fn kind(&self) -> PeerEventKind {
        match self {
            Self::Started(_) => PeerEventKind::Started,
            Self::Stopped(_) => PeerEventKind::Stopped,
            Self::Updated(_) => PeerEventKind::Updated,
            Self::Decomissionned(_) => PeerEventKind::Decomissionned,
        }
    }
}
