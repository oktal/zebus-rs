use std::any::TypeId;

use crate::{BindingKey, MessageTypeId, MessageTypeDescriptor};
use crate::{Message, MessageBinding};

pub(crate) mod proto {
    #[derive(Clone, prost::Message)]
    pub struct Subscription {
        #[prost(message, required, tag = "1")]
        pub message_type_id: crate::proto::MessageTypeId,

        #[prost(message, required, tag = "2")]
        pub binding_key: crate::proto::BindingKey,
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Subscription {
    message_type_id: MessageTypeId,
    binding_key: BindingKey
}

impl Subscription {
    pub fn bind<M: Message + MessageBinding + 'static>(
        bind_fn: impl FnOnce(&mut <M as MessageBinding>::Binding),
    ) -> Self {
        let mut binding = M::Binding::default();
        bind_fn(&mut binding);
        let binding_key = M::bind(binding).into();

        Self::with_binding::<M>(binding_key)
    }

    pub fn any<M: Message + MessageBinding + 'static>() -> Subscription {
        Self::with_binding::<M>(M::bind(M::Binding::default()).into())
    }

    pub fn with_binding<M: Message + 'static>(binding_key: BindingKey) -> Self {
        let message_descriptor = MessageTypeDescriptor {
            full_name: M::name().to_string(),
            r#type: TypeId::of::<M>(),
            is_persistent: !M::TRANSIENT,
            is_infrastructure: M::INFRASTRUCTURE
        };

        let message_type_id = MessageTypeId::from_descriptor(message_descriptor);

        Self {
            message_type_id,
            binding_key
        }
    }

    pub fn binding(&self) -> &BindingKey {
        &self.binding_key
    }

    pub fn full_name(&self) -> &str {
        self.message_type_id.full_name()
    }
}

#[cfg(test)]
mod tests {
    use zebus_core::binding_key;

    use super::*;
    use crate::BindingKey;

    #[derive(crate::Command)]
    #[zebus(namespace = "Abc.Test")]
    struct RoutableCommand {
        #[zebus(routing_position = 1)]
        name: String,

        #[zebus(routing_position = 2)]
        id: u32,
    }

    #[derive(crate::Command)]
    #[zebus(namespace = "Abc.Test")]
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
