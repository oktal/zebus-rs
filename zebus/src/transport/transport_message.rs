use prost::bytes::BufMut;

use crate::core::{MessagePayload, RawMessage};
use crate::proto::{FromProtobuf, IntoProtobuf};
use crate::{Message, MessageFlags, MessageId, MessageKind, MessageTypeId, Peer, PeerId};

use super::OriginatorInfo;

pub(crate) mod proto {
    use crate::{core::MessagePayload, proto, transport::OriginatorInfo, PeerId};

    /// Protobuf definition for a message sent through the bus
    #[derive(prost::Message, Clone)]
    pub struct TransportMessage {
        /// Id of the message
        #[prost(message, required, tag = 1)]
        pub id: proto::MessageId,

        /// Type of the message
        #[prost(message, required, tag = 2)]
        pub message_type_id: proto::MessageTypeId,

        /// Content bytes of the message
        #[prost(bytes, required, tag = 3)]
        pub content: Vec<u8>,

        /// Originator of the message
        #[prost(message, required, tag = 4)]
        pub originator: OriginatorInfo,

        /// Optional bus environment from which the message was sent
        #[prost(string, optional, tag = 5)]
        pub environment: Option<String>,

        /// Flag that indicates whether the message was sent to the persistence
        /// Used to instruct a receiver that the message was sent to the persistence
        /// and needs acknowledgment
        #[prost(bool, optional, tag = 6)]
        pub was_persisted: Option<bool>,

        /// List of recipient peers for this message that are persistent
        #[prost(message, repeated, tag = 7)]
        pub persistent_peer_ids: Vec<PeerId>,
    }

    impl MessagePayload for TransportMessage {
        fn message_type(&self) -> Option<&str> {
            Some(self.message_type_id.full_name.as_str())
        }

        fn content(&self) -> Option<&[u8]> {
            Some(&self.content)
        }
    }
}

/// A message that can be sent through the bus
#[derive(Debug, Clone)]
pub struct TransportMessage {
    /// Id of the message
    pub id: MessageId,

    /// Raw content bytes of the message
    pub content: RawMessage,

    /// Information about the originator of the message
    pub originator: OriginatorInfo,

    /// Environment from which the message was sent
    pub environment: Option<String>,

    /// Kind of message if known. Can be either Command or Event
    pub kind: Option<MessageKind>,

    /// Flags associated with the message
    pub flags: MessageFlags,

    /// Flag that indicates whether the message was sent to the persistence
    /// Used to instruct a receiver that the message was sent to the persistence
    /// and needs acknowledgment
    pub was_persisted: bool,

    /// List of recipient peers for this message that are persistent
    pub persistent_peer_ids: Vec<PeerId>,
}

impl TransportMessage {
    pub(crate) fn create(
        sender: &Peer,
        environment: String,
        message: &dyn Message,
    ) -> (uuid::Uuid, Self) {
        let uuid = uuid::Uuid::new_v4();
        let id = MessageId::from(uuid.clone());

        let kind = message.kind();
        let flags = message.flags();

        let content = RawMessage::encode(message);

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
                id,
                content,
                originator,
                environment: Some(environment),
                was_persisted: false,
                persistent_peer_ids: vec![],
                kind: Some(kind),
                flags,
            },
        )
    }

    pub(crate) fn is_persistent(&self) -> bool {
        !self.flags.contains(MessageFlags::TRANSIENT)
    }

    pub(crate) fn encode_to_vec(self) -> Vec<u8> {
        use prost::Message;
        self.into_protobuf().encode_to_vec()
    }

    pub(crate) fn encode<B>(self, buf: &mut B) -> Result<(), prost::EncodeError>
    where
        B: BufMut,
    {
        use prost::Message;
        self.into_protobuf().encode(buf)
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<TransportMessage, prost::DecodeError> {
        use prost::Message;
        proto::TransportMessage::decode(buf).map(TransportMessage::from_protobuf)
    }
}

impl MessagePayload for TransportMessage {
    fn message_type(&self) -> Option<&str> {
        Some(self.content.message_type().full_name())
    }

    fn content(&self) -> Option<&[u8]> {
        Some(self.content.content())
    }
}

impl IntoProtobuf for TransportMessage {
    type Output = proto::TransportMessage;

    fn into_protobuf(self) -> Self::Output {
        let (message_type, content) = self.content.into();

        proto::TransportMessage {
            id: self.id.into_protobuf(),
            message_type_id: message_type.into_protobuf(),
            content,
            originator: self.originator,
            environment: self.environment,
            was_persisted: Some(false),
            persistent_peer_ids: vec![],
        }
    }
}

impl FromProtobuf for TransportMessage {
    type Input = proto::TransportMessage;
    type Output = TransportMessage;

    fn from_protobuf(input: Self::Input) -> Self::Output {
        let message_type = MessageTypeId::from_protobuf(input.message_type_id);

        TransportMessage {
            id: MessageId::from_protobuf(input.id),
            content: RawMessage::new(message_type, input.content),
            originator: input.originator,
            environment: input.environment,
            was_persisted: input.was_persisted.unwrap_or(false),
            persistent_peer_ids: input.persistent_peer_ids,
            kind: None,
            flags: MessageFlags::default(),
        }
    }
}
