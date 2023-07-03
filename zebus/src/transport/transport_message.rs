use crate::core::MessagePayload;
use crate::message_type_id::MessageTypeId;
use crate::proto::{self, IntoProtobuf};
use crate::{Message, Peer};

use crate::{transport::OriginatorInfo, MessageId, PeerId};

/// Envelope for a message send through the bus
#[derive(prost::Message, Clone)]
pub struct TransportMessage {
    /// Id of the message
    #[prost(message, required, tag = "1")]
    pub id: proto::MessageId,

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

impl TransportMessage {
    /// Returns `true` if this message is persistent
    pub fn is_persistent(&self) -> bool {
        self.persistent_peer_ids.len() > 0
    }

    pub(crate) fn create(
        sender: &Peer,
        environment: String,
        message: &dyn Message,
    ) -> (uuid::Uuid, Self) {
        let uuid = uuid::Uuid::new_v4();
        let id = MessageId::from(uuid.clone());
        let message_type_id = MessageTypeId::of_val(message).into_protobuf();

        // TODO(oktal): Reuse buffer
        let content = message.encode_to_vec();

        let sender_id = sender.id.clone();
        let sender_endpoint = sender.endpoint.clone();

        // TODO(oktal): Correctly fill that up
        let originator = OriginatorInfo {
            sender_id,
            sender_endpoint,
            sender_machine_name: None,
            initiator_user_name: None,
        };

        // Create TransportMessage
        (
            uuid,
            Self {
                id: id.into_protobuf(),
                message_type_id,
                content,
                originator,
                environment: Some(environment),
                was_persisted: Some(false),
                persistent_peer_ids: vec![],
            },
        )
    }
}

impl MessagePayload for TransportMessage {
    fn message_type(&self) -> Option<&str> {
        Some(self.message_type_id.full_name.as_str())
    }

    fn content(&self) -> Option<&[u8]> {
        Some(&self.content[..])
    }
}
