use crate::{proto, Message};

#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus.Core")]
pub struct MessageExecutionCompleted {
    #[prost(message, required, tag = 1)]
    pub source_command_id: crate::MessageId,

    #[prost(int32, required, tag = 2)]
    pub error_code: i32,

    #[prost(message, optional, tag = 3)]
    pub payload_type_id: Option<proto::MessageTypeId>,

    #[prost(bytes, optional, tag = 4)]
    pub payload_bytes: Option<Vec<u8>>,
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
        if let Some(ref payload) = self.payload_bytes {
            self.is::<M>().then_some(M::decode(&payload[..]))
        } else {
            None
        }
    }
}
