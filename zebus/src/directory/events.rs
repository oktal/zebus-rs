use crate::{proto::bcl, PeerId};

use super::PeerDescriptor;
//
// TODO(oktal): Add transient
/// [`Event`] raised when a new peer has been started
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerStarted {
    /// [`PeerDescriptor`] descriptor of the peer that started
    #[prost(message, required, tag = 1)]
    descriptor: PeerDescriptor,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer has been stopped
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerStopped {
    /// [`PeerId`] id of the stopped peer
    #[prost(message, required, tag = 1)]
    id: PeerId,

    /// Endpoint of the stopped peer
    #[prost(string, optional, tag = 2)]
    endpoint: Option<String>,

    /// UTC timestamp when the peer was stopped
    #[prost(message, optional, tag = 3)]
    timestamp_utc: Option<bcl::DateTime>,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer has been decomissioned
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerDecommissioned {
    /// [`PeerId`] id of the decommissioned peer
    #[prost(message, required, tag = 1)]
    id: PeerId,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer is not responding
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerNotResponding {
    /// [`PeerId`] id of the peer that is not responding
    #[prost(message, required, tag = 1)]
    id: PeerId,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer is responding
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerResponding {
    /// [`PeerId`] id of the peer that is responding
    #[prost(message, required, tag = 1)]
    id: PeerId,
}
