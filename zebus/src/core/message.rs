/// Trait for types that expose raw content of protobuf-encoded messages
pub trait MessagePayload {
    /// Get the string representation of the message type
    fn message_type(&self) -> Option<&str>;

    /// Get the raw bytes of the protobuf-encoded messages
    fn content(&self) -> Option<&[u8]>;

    /// Returns `true` if the protobuf-encoded message is of type `M`
    fn is<M: crate::Message>(&self) -> bool {
        self.message_type().map(|s| s == M::name()).unwrap_or(false)
    }

    /// Attempt to decode the protobuf-encoded message as a message of type `M`
    fn decode_as<M: crate::Message + prost::Message + Default>(
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

/// A raw protobuf-encoded message along with its associated [`MessageType`]
#[derive(Debug, Eq, PartialEq)]
pub struct RawMessage<MessageType>(MessageType, Vec<u8>);

impl<MessageType> Into<(MessageType, Vec<u8>)> for RawMessage<MessageType> {
    fn into(self) -> (MessageType, Vec<u8>) {
        (self.0, self.1)
    }
}

impl<MessageType> RawMessage<MessageType> {
    pub(crate) fn new(message_type: impl Into<MessageType>, payload: impl Into<Vec<u8>>) -> Self {
        Self(message_type.into(), payload.into())
    }
}

impl<MessageType: AsRef<str>> MessagePayload for RawMessage<MessageType> {
    fn message_type(&self) -> Option<&str> {
        Some(self.0.as_ref())
    }

    fn content(&self) -> Option<&[u8]> {
        Some(&self.1[..])
    }
}
