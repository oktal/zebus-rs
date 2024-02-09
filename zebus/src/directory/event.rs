use crate::PeerId;
use super::PeerDescriptor;

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
    Started(PeerDescriptor),

    /// Peer been stopped
    Stopped(PeerDescriptor),

    /// Peer has been updated
    Updated(PeerDescriptor),

    /// Peer has been decomissionned
    Decomissionned(PeerDescriptor),
}

impl PeerEvent {
    pub(crate) fn descriptor(&self) -> &PeerDescriptor {
        match self {
            PeerEvent::Started(descriptor)
            | PeerEvent::Stopped(descriptor)
            | PeerEvent::Updated(descriptor)
            | PeerEvent::Decomissionned(descriptor) => descriptor,
        }
    }

    pub(crate) fn peer_id(&self) -> &PeerId {
        &self.descriptor().peer.id
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
