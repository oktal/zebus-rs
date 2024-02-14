use crate::{proto, PeerId};

#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence")]
pub(super) struct PersistMessageCommand {
    #[prost(message, required, tag = 1)]
    pub message: proto::TransportMessage,

    #[prost(message, repeated, tag = 2)]
    pub targets: Vec<PeerId>,
}

#[derive(prost::Message, crate::Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence", transient)]
pub(super) struct StartMessageReplayCommand {
    #[prost(message, required, tag = 1)]
    pub replay_id: proto::bcl::Guid,
}
