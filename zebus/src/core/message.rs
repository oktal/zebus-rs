use crate::{Message, MessageDescriptor, MessageTypeId};

/// Trait for types that expose raw content of protobuf-encoded messages
pub trait MessagePayload {
    /// Get the string representation of the message type
    fn message_type(&self) -> Option<&str>;

    /// Get the raw bytes of the protobuf-encoded messages
    fn content(&self) -> Option<&[u8]>;

    /// Returns `true` if the protobuf-encoded message is of type `M`
    fn is<M: MessageDescriptor>(&self) -> bool {
        self.message_type().map(|s| s == M::name()).unwrap_or(false)
    }

    /// Attempt to decode the protobuf-encoded message as a message of type `M`
    fn decode_as<M: MessageDescriptor + prost::Message + Default>(
        &self,
    ) -> Option<Result<M, prost::DecodeError>> {
        // NOTE(oktal): maybe one day Rust will get `and_then` on `bool`
        if self.is::<M>() {
            self.content().map(|bytes| M::decode(bytes))
        } else {
            None
        }
    }
}

/// A raw protobuf-encoded message along with its associated [`MessageTypeId`]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RawMessage(MessageTypeId, Vec<u8>);

impl Into<(MessageTypeId, Vec<u8>)> for RawMessage {
    fn into(self) -> (MessageTypeId, Vec<u8>) {
        (self.0, self.1)
    }
}

impl RawMessage {
    pub(crate) fn new(message_type: impl Into<MessageTypeId>, payload: impl Into<Vec<u8>>) -> Self {
        Self(message_type.into(), payload.into())
    }

    pub(crate) fn encode(msg: &dyn Message) -> Self {
        // TODO(oktal): reuse buffer
        let content = msg.encode_to_vec();
        Self(MessageTypeId::of_val(msg), content)
    }

    pub fn message_type(&self) -> &MessageTypeId {
        &self.0
    }

    pub fn content(&self) -> &[u8] {
        &self.1
    }
}

impl MessagePayload for RawMessage {
    fn message_type(&self) -> Option<&str> {
        Some(self.0.full_name())
    }

    fn content(&self) -> Option<&[u8]> {
        Some(self.content())
    }
}
