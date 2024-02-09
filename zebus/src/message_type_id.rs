use crate::{
    proto::{FromProtobuf, IntoProtobuf},
    Message, MessageDescriptor, MessageTypeDescriptor,
};
use std::{
    any::TypeId,
    hash::{Hash, Hasher},
};

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
enum Repr {
    /// A message type with is associated complete [`MessageTypeDescriptor`]
    Descriptor(MessageTypeDescriptor),

    /// Name of the message
    Name(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageTypeId {
    repr: Repr,
}

impl Hash for MessageTypeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        println!("Hashing {:?} to {}", self, self.full_name());
        self.full_name().hash(state);
    }
}

impl From<MessageTypeDescriptor> for MessageTypeId {
    fn from(value: MessageTypeDescriptor) -> Self {
        Self {
            repr: Repr::Descriptor(value),
        }
    }
}

impl AsRef<str> for MessageTypeId {
    fn as_ref(&self) -> &str {
        self.full_name()
    }
}

impl MessageTypeId {
    pub fn of<M: MessageDescriptor + 'static>() -> Self {
        Self::from(MessageTypeDescriptor::of::<M>())
    }

    pub fn of_val(message: &dyn Message) -> Self {
        Self::from(MessageTypeDescriptor::of_val(message.up()))
    }

    pub fn is<M: MessageDescriptor>(&self) -> bool {
        self.full_name() == M::name()
    }

    /// Returns the fully qualified name of this message type
    pub fn full_name(&self) -> &str {
        match &self.repr {
            Repr::Descriptor(desc) => &desc.full_name,
            Repr::Name(name) => name.as_str(),
        }
    }

    /// Returns the Rust [`TypeId`] type representation of this message type
    pub fn type_id(&self) -> Option<TypeId> {
        self.descriptor().map(|d| d.r#type)
    }

    /// Returns `true` if this message type is an infrastucture message
    pub fn is_infrastructure(&self) -> Option<bool> {
        self.descriptor().map(|d| d.is_infrastructure)
    }

    /// Returns `true` if this message type is persistent
    pub fn is_persistent(&self) -> Option<bool> {
        self.descriptor().map(|d| d.is_persistent)
    }

    fn descriptor(&self) -> Option<&MessageTypeDescriptor> {
        match &self.repr {
            Repr::Descriptor(descriptor) => Some(descriptor),
            Repr::Name(_) => None,
        }
    }

    pub(crate) fn into_name(self) -> String {
        match self.repr {
            Repr::Descriptor(desc) => desc.full_name.to_string(),
            Repr::Name(n) => n,
        }
    }
}

impl IntoProtobuf for MessageTypeId {
    type Output = proto::MessageTypeId;

    fn into_protobuf(self) -> Self::Output {
        let full_name = self.full_name().to_string();
        proto::MessageTypeId { full_name }
    }
}

impl FromProtobuf for MessageTypeId {
    type Input = proto::MessageTypeId;

    fn from_protobuf(input: Self::Input) -> Self {
        Self {
            repr: Repr::Name(input.full_name),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;
    use crate::{Event, MessageTypeId};

    #[derive(Event)]
    #[zebus(namespace = "Abc.Test")]
    struct TestEvent {}

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn hash() {
        let type_id_descriptor = MessageTypeId::of::<TestEvent>();
        let type_id_name = MessageTypeId::from_protobuf(proto::MessageTypeId {
            full_name: type_id_descriptor.full_name().to_string(),
        });

        assert_eq!(
            calculate_hash(&type_id_descriptor),
            calculate_hash(&type_id_name)
        );
    }
}
