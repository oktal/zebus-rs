use crate::proto::{self, IntoProtobuf};
use crate::{Message, Peer};

use crate::{transport::OriginatorInfo, MessageId, PeerId};

/// Envelope for a message send through the bus
#[derive(Clone, prost::Message)]
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

    pub(crate) fn create<M: Message + prost::Message>(
        sender: &Peer,
        environment: String,
        message: &M,
    ) -> (uuid::Uuid, Self) {
        let uuid = uuid::Uuid::new_v4();
        let id = MessageId::from(uuid.clone());
        let message_type_id = crate::proto::MessageTypeId {
            full_name: M::name().to_string(),
        };

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

    pub(crate) fn is<M: Message>(&self) -> bool {
        self.message_type_id.is::<M>()
    }

    pub(crate) fn decode_as<M: Message + prost::Message + Default>(
        &self,
    ) -> Option<Result<M, prost::DecodeError>> {
        self.is::<M>().then_some(M::decode(&self.content[..]))
    }
}
