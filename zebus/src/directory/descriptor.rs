use chrono::Utc;

use crate::{proto::IntoProtobuf, Peer, Subscription};

pub(crate) mod proto {
    /// Description of a [`Peer`]
    #[derive(Clone, prost::Message)]
    pub struct PeerDescriptor {
        #[prost(message, required, tag = 1)]
        pub peer: crate::Peer,

        #[prost(message, repeated, tag = 2)]
        pub subscriptions: Vec<crate::proto::Subscription>,

        #[prost(bool, required, tag = 3)]
        pub is_persistent: bool,

        #[prost(message, optional, tag = 4)]
        pub timestamp_utc: Option<crate::proto::bcl::DateTime>,

        #[prost(bool, optional, tag = 5)]
        pub has_debugger_attached: Option<bool>,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerDescriptor {
    pub peer: Peer,

    pub subscriptions: Vec<Subscription>,

    pub is_persistent: bool,

    pub timestamp_utc: Option<chrono::DateTime<Utc>>,

    pub has_debugger_attached: Option<bool>,
}

impl IntoProtobuf for PeerDescriptor {
    type Output = proto::PeerDescriptor;

    fn into_protobuf(self) -> Self::Output {
        proto::PeerDescriptor {
            peer: self.peer,
            subscriptions: self.subscriptions.into_protobuf(),
            is_persistent: self.is_persistent,
            timestamp_utc: self.timestamp_utc.into_protobuf(),
            has_debugger_attached: self.has_debugger_attached.clone(),
        }
    }
}
