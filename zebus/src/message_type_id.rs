use crate::{proto::IntoProtobuf, Message, MessageDescriptor, MessageTypeDescriptor};
use std::any::TypeId;

pub(crate) mod proto {
    use crate::{Message, MessageDescriptor};

    #[derive(Clone, prost::Message, Eq, PartialEq, Hash)]
    pub struct MessageTypeId {
        #[prost(string, tag = "1")]
        pub full_name: String,
    }

    impl MessageTypeId {
        pub fn of<M: MessageDescriptor>() -> Self {
            Self {
                full_name: M::name().to_string(),
            }
        }

        pub fn of_val(message: &dyn Message) -> Self {
            Self {
                full_name: message.name().to_string(),
            }
        }

        pub fn is<M: MessageDescriptor>(&self) -> bool {
            self.full_name == M::name()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageTypeId {
    descriptor: MessageTypeDescriptor,
}

impl From<MessageTypeDescriptor> for MessageTypeId {
    fn from(value: MessageTypeDescriptor) -> Self {
        Self { descriptor: value }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MessageType(String);

impl AsRef<str> for MessageType {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl MessageType {
    pub fn of<M: MessageDescriptor>() -> Self {
        Self(M::name().to_string())
    }

    pub fn of_val(message: &dyn Message) -> Self {
        Self(message.name().to_string())
    }

    pub fn is<M: MessageDescriptor>(&self) -> bool {
        self.0 == M::name()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for MessageType {
    fn from(str: String) -> Self {
        Self(str)
    }
}

impl From<MessageTypeId> for MessageType {
    fn from(id: MessageTypeId) -> Self {
        Self(id.descriptor.full_name.to_string())
    }
}

impl From<MessageTypeDescriptor> for MessageType {
    fn from(descriptor: MessageTypeDescriptor) -> Self {
        Self(descriptor.full_name.to_string())
    }
}

impl From<proto::MessageTypeId> for MessageType {
    fn from(id: proto::MessageTypeId) -> Self {
        Self(id.full_name)
    }
}

impl MessageTypeId {
    pub(crate) fn from_descriptor(descriptor: MessageTypeDescriptor) -> Self {
        Self { descriptor }
    }

    pub fn of<M: MessageDescriptor + 'static>() -> Self {
        Self::from_descriptor(MessageTypeDescriptor::of::<M>())
    }

    pub fn of_val(message: &dyn Message) -> Self {
        Self::from_descriptor(MessageTypeDescriptor::of_val(message))
    }

    pub fn is<M: MessageDescriptor>(&self) -> bool {
        self.descriptor.full_name == M::name()
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
            full_name: self.descriptor.full_name.to_string(),
        }
    }
}

impl IntoProtobuf for MessageTypeId {
    type Output = proto::MessageTypeId;

    fn into_protobuf(self) -> Self::Output {
        proto::MessageTypeId {
            full_name: self.descriptor.full_name.to_string(),
        }
    }
}
