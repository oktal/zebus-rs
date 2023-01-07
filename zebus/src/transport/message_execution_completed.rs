use crate::{core::MessagePayload, proto};

/// Message sent to notify completion of a command
#[derive(Clone, prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Core")]
pub struct MessageExecutionCompleted {
    /// [`proto::MessageId`] of the original command
    #[prost(message, required, tag = 1)]
    pub command_id: proto::MessageId,

    /// Response error code
    #[prost(int32, required, tag = 2)]
    pub error_code: i32,

    /// Optional response [`proto::MessageTypeId`] message type id
    #[prost(message, optional, tag = 3)]
    pub payload_type_id: Option<proto::MessageTypeId>,

    /// Optional response payload bytes
    #[prost(bytes, optional, tag = 4)]
    pub payload: Option<Vec<u8>>,

    /// Optional response message
    #[prost(string, optional, tag = 5)]
    pub response_message: Option<String>,
}

impl MessagePayload for MessageExecutionCompleted {
    fn message_type(&self) -> Option<&str> {
        self.payload_type_id.as_ref().map(|m| m.full_name.as_str())
    }

    fn content(&self) -> Option<&[u8]> {
        self.payload.as_ref().map(|p| &p[..])
    }
}
