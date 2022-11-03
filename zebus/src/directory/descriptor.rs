use crate::{proto, Peer};

/// Description for a [`Peer`]
#[derive(Clone, prost::Message)]
pub struct PeerDescriptor {
    #[prost(message, required, tag = 1)]
    pub peer: Peer,

    #[prost(message, repeated, tag = 2)]
    pub subscriptions: Vec<proto::Subscription>,

    #[prost(bool, required, tag = 3)]
    pub is_persistent: bool,

    #[prost(message, optional, tag = 4)]
    pub timestamp_utc: Option<proto::bcl::DateTime>,

    #[prost(bool, optional, tag = 5)]
    pub has_debugger_attached: Option<bool>,
}
