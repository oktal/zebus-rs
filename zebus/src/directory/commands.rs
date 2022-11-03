use super::PeerDescriptor;

/// [`Command`] to send to register a [`Peer`] to the directory
#[derive(Clone, prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure, routable = true)]
pub(super) struct RegisterPeerCommand {
    #[prost(message, required, tag = "1")]
    /// [`PeerDescriptor`] description of the peer to register
    pub peer: PeerDescriptor,
}

/// Response of the [`RegisterPeerCommand`] command
#[derive(prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub(crate) struct RegisterPeerResponse {
    /// List of [`PeerDescriptor`] peers currently registered to the directory
    #[prost(message, repeated, tag = 1)]
    pub peers: Vec<PeerDescriptor>,
}

// TODO(oktal): remove limitation for struct with unnamed fields on `Command` and `Event` derive
// macros
/// [`Command`] to send a PING to a peer
#[derive(prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub struct PingPeerCommand {}
