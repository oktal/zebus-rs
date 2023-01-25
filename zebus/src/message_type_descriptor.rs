use crate::{
    message_type_id::proto, proto::IntoProtobuf, Message, MessageDescriptor, MessageFlags,
};
use std::any::TypeId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageTypeDescriptor {
    /// Fully-qualified name of the message
    pub(crate) full_name: String,

    /// Rust type representation of the message
    pub(crate) r#type: TypeId,

    /// Marker flag for a persistent message
    pub(crate) is_persistent: bool,

    /// Market flag for an infrastructure message
    pub(crate) is_infrastructure: bool,
    // TODO(oktal): Handle routing fields info
}

impl MessageTypeDescriptor {
    pub(crate) fn of<M: MessageDescriptor + 'static>() -> Self {
        let flags = M::flags();

        Self {
            full_name: M::name().to_string(),
            r#type: TypeId::of::<M>(),
            is_persistent: !flags.contains(MessageFlags::TRANSIENT),
            is_infrastructure: flags.contains(MessageFlags::INFRASTRUCTURE),
        }
    }

    pub(crate) fn of_val(message: &dyn Message) -> Self {
        let flags = message.flags();

        Self {
            full_name: message.name().to_string(),
            r#type: message.type_id(),
            is_persistent: !flags.contains(MessageFlags::TRANSIENT),
            is_infrastructure: flags.contains(MessageFlags::INFRASTRUCTURE),
        }
    }
}

impl AsRef<str> for MessageTypeDescriptor {
    fn as_ref(&self) -> &str {
        self.full_name.as_str()
    }
}

impl IntoProtobuf for MessageTypeDescriptor {
    type Output = proto::MessageTypeId;

    fn into_protobuf(self) -> Self::Output {
        proto::MessageTypeId {
            full_name: self.full_name,
        }
    }
}
