use crate::proto;

/// [`Command`] to send to register a [`Peer`] to the directory
#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure)]
pub(crate) struct RegisterPeerCommand {
    /// [`PeerDescriptor`] description of the peer to register
    #[prost(message, required, tag = 1)]
    pub peer: proto::PeerDescriptor,
}

/// Response of the [`RegisterPeerCommand`] command
#[derive(prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub(crate) struct RegisterPeerResponse {
    /// List of [`PeerDescriptor`] peers currently registered to the directory
    #[prost(message, repeated, tag = 1)]
    pub peers: Vec<proto::PeerDescriptor>,
}

#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub(crate) struct UnregisterPeerCommand {
    /// Identifier of the peer to unregister
    #[prost(message, required, tag = 1)]
    pub peer_id: crate::PeerId,

    /// Endpoint of the peer to unregister
    #[prost(string, optional, tag = 2)]
    pub endpoint: Option<String>,

    #[prost(message, optional, tag = 3)]
    pub timestamp: Option<proto::bcl::DateTime>,
}

// TODO(oktal): remove limitation for struct with unnamed fields on `Command` and `Event` derive
// macros
/// [`Command`] to send a PING to a peer
#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure)]
pub struct PingPeerCommand {}
