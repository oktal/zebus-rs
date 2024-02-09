use crate::directory::binding::MessageBinding;
use crate::proto::{FromProtobuf, IntoProtobuf};
use crate::{BindingExpression, MessageDescriptor};
use crate::{BindingKey, MessageTypeId};

pub(crate) mod proto {
    #[derive(Clone, prost::Message)]
    pub struct Subscription {
        #[prost(message, required, tag = 1)]
        pub message_type_id: crate::proto::MessageTypeId,

        #[prost(message, required, tag = 2)]
        pub binding_key: crate::proto::BindingKey,
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Subscription {
    pub(crate) message_type_id: MessageTypeId,
    pub(crate) binding_key: BindingKey,
}

impl Subscription {
    pub(crate) fn new(message_type_id: impl Into<MessageTypeId>, binding_key: BindingKey) -> Self {
        Self {
            message_type_id: message_type_id.into(),
            binding_key,
        }
    }

    pub fn bind<M: MessageDescriptor + BindingExpression + 'static>(
        bind_fn: impl FnOnce(&mut <M as BindingExpression>::Binding),
    ) -> Self {
        let mut binding = M::Binding::default();
        bind_fn(&mut binding);
        let binding_key = M::bind(binding).into();

        Self::with_binding::<M>(binding_key)
    }

    pub fn any<M: MessageDescriptor + BindingExpression + 'static>() -> Subscription {
        Self::with_binding::<M>(M::bind(M::Binding::default()).into())
    }

    pub fn with_binding<M: MessageDescriptor + 'static>(binding_key: BindingKey) -> Self {
        let message_type_id = MessageTypeId::of::<M>();

        Self {
            message_type_id,
            binding_key,
        }
    }

    pub fn binding(&self) -> &BindingKey {
        &self.binding_key
    }

    pub fn full_name(&self) -> &str {
        self.message_type_id.full_name()
    }

    pub(crate) fn message_type(&self) -> &MessageTypeId {
        &self.message_type_id
    }
}

impl From<MessageBinding> for Subscription {
    fn from(value: MessageBinding) -> Self {
        Subscription::new(value.descriptor, value.key)
    }
}

impl IntoProtobuf for Subscription {
    type Output = proto::Subscription;

    fn into_protobuf(self) -> Self::Output {
        proto::Subscription {
            message_type_id: self.message_type_id.into_protobuf(),
            binding_key: self.binding_key.into_protobuf(),
        }
    }
}

impl FromProtobuf for Subscription {
    type Input = proto::Subscription;

    fn from_protobuf(input: Self::Input) -> Self {
        let message_type_id = MessageTypeId::from_protobuf(input.message_type_id);
        let binding_key = BindingKey::from_protobuf(input.binding_key);

        Subscription {
            message_type_id,
            binding_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zebus_core::binding_key;

    #[derive(crate::Command)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct RoutableCommand {
        #[zebus(routing_position = 1)]
        name: String,

        #[zebus(routing_position = 2)]
        id: u32,
    }

    #[derive(crate::Command)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct UnorderedRoutableCommand {
        #[zebus(routing_position = 3)]
        name: String,

        #[zebus(routing_position = 1)]
        id: u32,

        #[zebus(routing_position = 2)]
        flag: bool,
    }

    #[test]
    fn subscription_bind() {
        let subscription = Subscription::bind::<RoutableCommand>(|binding| {
            binding.name.matches("Hello".to_string());
            binding.id.any();
        });

        assert_eq!(subscription.full_name(), "Abc.Test.RoutableCommand");
        assert_eq!(subscription.binding(), &binding_key!["Hello", *].into());
    }

    #[test]
    fn subscription_any() {
        let subscription = Subscription::any::<RoutableCommand>();

        assert_eq!(subscription.full_name(), "Abc.Test.RoutableCommand");
        assert_eq!(subscription.binding(), &binding_key![*, *].into());
    }
}
