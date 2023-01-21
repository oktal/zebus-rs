use crate::{proto::{self, prost}, PeerId};

// TODO(oktal): Add transient
/// [`Event`] raised when a new peer has been started
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerStarted {
    /// [`PeerDescriptor`] descriptor of the peer that started
    #[prost(message, required, tag = 1)]
    pub descriptor: proto::PeerDescriptor,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer has been stopped
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerStopped {
    /// [`PeerId`] id of the stopped peer
    #[prost(message, required, tag = 1)]
    pub id: PeerId,

    /// Endpoint of the stopped peer
    #[prost(string, optional, tag = 2)]
    pub endpoint: Option<String>,

    /// UTC timestamp when the peer was stopped
    #[prost(message, optional, tag = 3)]
    pub timestamp_utc: Option<proto::bcl::DateTime>,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer has been decomissioned
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerDecommissioned {
    /// [`PeerId`] id of the decommissioned peer
    #[prost(message, required, tag = 1)]
    pub id: PeerId,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer is not responding
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerNotResponding {
    /// [`PeerId`] id of the peer that is not responding
    #[prost(message, required, tag = 1)]
    pub id: PeerId,
}

// TODO(oktal): Add transient
/// [`Event`] raised when a peer is responding
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerResponding {
    /// [`PeerId`] id of the peer that is responding
    #[prost(message, required, tag = 1)]
    pub id: PeerId,
}

#[derive(prost::Message, Clone)]
/// List of subscriptions for a given message
pub struct SubscriptionsForType {
    /// Message the subscriptions belong to
    #[prost(message, required, tag = 1)]
    pub message_type: proto::MessageTypeId,

    /// List of bindings for this message
    #[prost(message, repeated, tag = 2)]
    pub bindings: Vec<proto::BindingKey>,
}

/// TODO(oktal): Add transient
/// [`Event`] raised when subscriptions for a message have been updated
#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PeerSubscriptionsForTypeUpdated {
    /// [`PeerId`] id of the peer for which the subscriptions have been updated
    #[prost(message, required, tag = 1)]
    pub peer_id: PeerId,

    /// List of subscriptions that have been updated
    #[prost(message, repeated, tag = 2)]
    pub subscriptions: Vec<SubscriptionsForType>,

    /// Timestamp of the update
    #[prost(message, required, tag = 3)]
    pub timestamp_utc: proto::bcl::DateTime,
}
