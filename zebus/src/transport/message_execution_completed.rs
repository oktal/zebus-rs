use crate::{proto, Message};

/// Message sent to notify completion of a command
#[derive(Clone, prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Core")]
pub struct MessageExecutionCompleted {
    /// [`MessageId`] of the original command
    #[prost(message, required, tag = 1)]
    pub command_id: proto::MessageId,

    /// Response error code
    #[prost(int32, required, tag = 2)]
    pub error_code: i32,

    /// Optional response [`MessageTypeId`] message type id
    #[prost(message, optional, tag = 3)]
    pub payload_type_id: Option<proto::MessageTypeId>,

    /// Optional response payload bytes
    #[prost(bytes, optional, tag = 4)]
    pub payload: Option<Vec<u8>>,

    /// Optional response message
    #[prost(string, optional, tag = 5)]
    pub response_message: Option<String>,
}

impl MessageExecutionCompleted {
    pub(crate) fn is<M: Message>(&self) -> bool {
        if let Some(ref payload_type_id) = self.payload_type_id {
            payload_type_id.is::<M>()
        } else {
            false
        }
    }

    pub(crate) fn decode_as<M: Message + prost::Message + Default>(
        &self,
    ) -> Option<Result<M, prost::DecodeError>> {
        if let Some(ref payload) = self.payload {
            self.is::<M>().then_some(M::decode(&payload[..]))
        } else {
            None
        }
    }
}
