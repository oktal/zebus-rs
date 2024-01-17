use crate::PeerId;

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

    pub fn kind(&self) -> PeerEventKind {
        match self {
            Self::Started(_) => PeerEventKind::Started,
            Self::Stopped(_) => PeerEventKind::Stopped,
            Self::Updated(_) => PeerEventKind::Updated,
            Self::Decomissionned(_) => PeerEventKind::Decomissionned,
        }
    }
}
