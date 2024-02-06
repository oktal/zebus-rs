use crate::{
    BindingExpression, BindingKey, Message, MessageDescriptor, MessageTypeDescriptor, Subscription,
};

/// Represents a [`Message`] message with an associated [`BindingKey`] binding key
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MessageBinding {
    /// Descriptor of the message
    pub(crate) descriptor: MessageTypeDescriptor,

    /// Binding key
    pub(crate) key: BindingKey,
}

impl MessageBinding {
    /// Create a new instance of a [`MessageBinding`]
    pub(crate) fn with_binding<M: MessageDescriptor + 'static>(
        binding: impl Into<BindingKey>,
    ) -> Self {
        let descriptor = MessageTypeDescriptor::of::<M>();
        let key = binding.into();

        Self { descriptor, key }
    }

    /// Create a [`MessageBinding`] for an instance of a [`Message`] that binds with the values of
    /// the routing fields of the [`Message`]
    pub fn of<M: Message + MessageDescriptor>(msg: &M) -> Self {
        Self::with_binding::<M>(msg.get_binding())
    }

    /// Create a [`MessageBinding`] for an instance of a [`Message`] that binds with the values of
    /// the routing fields of the [`Message`]
    pub fn of_val(msg: &dyn Message) -> Self {
        let descriptor = MessageTypeDescriptor::of_val(msg.up());
        let key = msg.get_binding().into();

        Self { descriptor, key }
    }

    /// Create a [`MessageBinding`] that generates a [`BindingKey`] using the provided [`BindingExpression`]
    pub fn bind<M: MessageDescriptor + BindingExpression + 'static>(
        bind_fn: impl FnOnce(&mut <M as BindingExpression>::Binding),
    ) -> Self {
        let mut binding = M::Binding::default();
        bind_fn(&mut binding);
        Self::with_binding::<M>(M::bind(binding))
    }

    /// Create a [`MessageBinding`] for any value of a [`Message`] routing fields
    pub fn any<M: MessageDescriptor + BindingExpression + 'static>() -> Self {
        Self::with_binding::<M>(M::bind(M::Binding::default()))
    }

    /// Return a reference to the [`MessageTypeDescriptor`] for the current binding
    pub fn descriptor(&self) -> &MessageTypeDescriptor {
        &self.descriptor
    }

    /// Return a reference to the [`BindingKey`] for the current binding
    pub fn key(&self) -> &BindingKey {
        &self.key
    }

    /// Create a [`Subscription`] from this binding
    pub fn subscription(&self) -> Subscription {
        Subscription::from(self.clone())
    }

    pub fn into_subscription(self) -> Subscription {
        Subscription::from(self)
    }
}

#[cfg(test)]
mod tests {
    use zebus_core::binding_key;

    use super::*;

    #[derive(prost::Message, crate::Command, Clone)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct RoutableCommand {
        #[prost(required, string)]
        #[zebus(routing_position = 1)]
        name: String,

        #[prost(required, fixed32)]
        #[zebus(routing_position = 2)]
        id: u32,

        #[prost(required, fixed32)]
        priority: u32,
    }

    #[test]
    fn generate_binding_from_instance_of_message() {
        // Create a routable command
        let cmd = RoutableCommand {
            name: "BrewCommand".into(),
            id: 0xC0FFEE,
            priority: 100,
        };

        // Generate a binding for the command
        let binding = MessageBinding::of(&cmd);

        // Make sure we generated a binding for the right message
        assert!(binding.descriptor.is::<RoutableCommand>());

        // Make sure we generated a binding key with the right values extracted from the values of the routable command fields
        assert_eq!(binding.key, binding_key!["BrewCommand", 0xC0FFEE].into());
    }

    #[test]
    fn generate_any_binding() {
        // Generate a binding for any value
        let binding = MessageBinding::any::<RoutableCommand>();

        // Make sure we generated a binding for the right message
        assert!(binding.descriptor.is::<RoutableCommand>());

        // Make sure we generated a binding for any value of the message routing fields
        assert_eq!(binding.key, binding_key![*, *].into());
    }
}
