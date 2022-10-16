use super::PeerDescriptor;

#[derive(prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory")]
pub(crate) struct RegisterPeerResponse {
    #[prost(message, repeated, tag = 1)]
    pub peers: Vec<PeerDescriptor>,
}
