use crate::{message_type_id::proto, transport::OriginatorInfo, MessageId, PeerId};

/// Envelope for a message send through the bus
#[derive(Clone, prost::Message)]
pub struct TransportMessage {
    /// Id of the message
    #[prost(message, required, tag = "1")]
    pub id: MessageId,

    /// Type of the message
    #[prost(message, required, tag = "2")]
    pub message_type_id: proto::MessageTypeId,

    /// Content bytes of the message
    #[prost(bytes, required, tag = "3")]
    pub content: Vec<u8>,

    /// Originator of the message
    #[prost(message, required, tag = "4")]
    pub originator: OriginatorInfo,

    /// Optional bus environment from which the message was sent
    #[prost(string, optional, tag = "5")]
    pub environment: Option<String>,

    #[prost(bool, optional, tag = "6")]
    pub was_persisted: Option<bool>,

    #[prost(message, repeated, tag = "7")]
    pub persistent_peer_ids: Vec<PeerId>,
}
