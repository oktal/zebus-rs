use crate::proto::{self, prost};

/// [`Command`] to send to register a [`Peer`] to the directory
#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure)]
pub(crate) struct RegisterPeerCommand {
    #[prost(message, required, tag = "1")]
    /// [`PeerDescriptor`] description of the peer to register
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

// TODO(oktal): remove limitation for struct with unnamed fields on `Command` and `Event` derive
// macros
/// [`Command`] to send a PING to a peer
#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure)]
pub struct PingPeerCommand {}
