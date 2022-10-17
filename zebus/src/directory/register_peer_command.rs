use crate::directory::PeerDescriptor;

#[derive(Clone, prost::Message, crate::Command)]
#[zebus(namespace = "Abc.Zebus.Directory", infrastructure, routable = true)]
pub(crate) struct RegisterPeerCommand {
    #[prost(message, required, tag = "1")]
    pub peer: PeerDescriptor,
}
