use crate::directory::PeerDescriptor;

#[derive(Clone, prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure, routable = true)]
pub(super) struct RegisterPeerCommand {
    #[prost(message, required, tag = "1")]
    pub peer: PeerDescriptor,
}
