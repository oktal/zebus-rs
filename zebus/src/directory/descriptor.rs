use chrono::Utc;

use crate::{
    proto::{FromProtobuf, IntoProtobuf},
    Peer, PeerId, Subscription,
};

use super::MessageBinding;

pub(crate) mod proto {
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

/// Description of a [`Peer`]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerDescriptor {
    /// The [`Peer`] this descriptor belongs to
    pub peer: Peer,

    /// List of [`Subscription`] subscriptions for this [`Peer`]
    pub subscriptions: Vec<Subscription>,

    /// Flag indicating whether the current [`Peer`] is persistent or not
    pub is_persistent: bool,

    /// Optional timestamp of the current [`Peer`]
    pub timestamp_utc: Option<chrono::DateTime<Utc>>,

    /// Optional flag indicating whether the current [`Peer`] has been started
    /// with a debugger attached
    pub has_debugger_attached: Option<bool>,
}

impl PeerDescriptor {
    /// Returns the current [`PeerId`] of this peer
    pub fn id(&self) -> &PeerId {
        &self.peer.id
    }

    /// Returns the current [`Peer`]
    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    /// Returns whether the current peer handles a given [`MessageBinding`]
    pub(crate) fn handles(&self, binding: &MessageBinding) -> bool {
        self.subscriptions.iter().any(|s| s.matches(binding))
    }
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

impl FromProtobuf for PeerDescriptor {
    type Input = proto::PeerDescriptor;

    fn from_protobuf(input: Self::Input) -> Self {
        let subscriptions = input
            .subscriptions
            .into_iter()
            .map(Subscription::from_protobuf)
            .collect();

        let timestamp_utc = input.timestamp_utc.map(FromProtobuf::from_protobuf);

        PeerDescriptor {
            peer: input.peer,
            subscriptions,
            is_persistent: input.is_persistent,
            timestamp_utc,
            has_debugger_attached: input.has_debugger_attached,
        }
    }
}
