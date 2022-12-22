use crate::{proto::IntoProtobuf, MessageTypeDescriptor};
use std::any::TypeId;

pub(crate) mod proto {
    use crate::Message;

    #[derive(Clone, prost::Message)]
    pub struct MessageTypeId {
        #[prost(string, tag = "1")]
        pub full_name: String,
    }

    impl MessageTypeId {
        pub fn is<M: Message>(&self) -> bool {
            self.full_name == M::name()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageTypeId {
    descriptor: MessageTypeDescriptor,
}

impl MessageTypeId {
    pub(crate) fn from_descriptor(descriptor: MessageTypeDescriptor) -> Self {
        Self { descriptor }
    }

    /// Returns the fully qualified name of this message type
    pub fn full_name(&self) -> &str {
        &self.descriptor.full_name
    }

    /// Returns the Rust [`TypeId`] type representation of this message type
    pub fn type_id(&self) -> TypeId {
        self.descriptor.r#type
    }

    /// Returns `true` if this message type is an infrastucture message
    pub fn is_infrastructure(&self) -> bool {
        self.descriptor.is_infrastructure
    }

    /// Returns `true` if this message type is persistent
    pub fn is_persistent(&self) -> bool {
        self.descriptor.is_persistent
    }

    pub(crate) fn into_protobuf(self) -> proto::MessageTypeId {
        proto::MessageTypeId {
            full_name: self.descriptor.full_name,
        }
    }
}

impl IntoProtobuf for MessageTypeId {
    type Output = proto::MessageTypeId;

    fn into_protobuf(self) -> Self::Output {
        proto::MessageTypeId {
            full_name: self.descriptor.full_name.clone(),
        }
    }
}
