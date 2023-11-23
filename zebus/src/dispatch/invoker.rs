use std::any::type_name;

use futures_core::future::BoxFuture;

use crate::core::{HandlerDescriptor, MessageDescriptor};
use crate::{BindingKey, DispatchHandler, MessageTypeDescriptor, Response, SubscriptionMode};

use super::DispatchContext;

/// A descriptor for an invoker for a specific type of message
#[derive(Debug, Clone)]
pub(crate) struct MessageInvokerDescriptor {
    /// The `type_name()` of the invoker
    pub invoker_type: &'static str,

    /// Name of the dispatch queue
    pub dispatch_queue: &'static str,

    /// Descriptor of the message type
    pub message: MessageTypeDescriptor,

    /// Mode of subscription for the message
    pub subscription_mode: SubscriptionMode,

    /// Subscription bindings for the message
    pub bindings: Vec<BindingKey>,
}

/// An invoker for specific message type
pub(crate) struct MessageHandlerInvoker<'a> {
    /// Descriptor of the invoker
    pub descriptor: &'a MessageInvokerDescriptor,

    /// A future that will call the invoker
    pub invoker: HandlerFuture,
}

impl MessageInvokerDescriptor {
    pub(crate) fn of<H, M>() -> Self
    where
        M: MessageDescriptor + 'static,
        H: DispatchHandler + HandlerDescriptor<M>,
    {
        Self {
            invoker_type: type_name::<H>(),
            dispatch_queue: H::DISPATCH_QUEUE,
            message: MessageTypeDescriptor::of::<M>(),
            subscription_mode: H::subscription_mode(),
            bindings: H::bindings().into_iter().map(Into::into).collect(),
        }
    }
}

/// Represents a [`Response`] for a specific handler type of a specific message
pub(crate) struct HandlerResponse(&'static str, Option<Response>);

impl HandlerResponse {
    pub(crate) fn for_handler<H: DispatchHandler>(response: Option<Response>) -> Self {
        Self(type_name::<H>(), response)
    }

    pub(super) fn handler_type(&self) -> &'static str {
        self.0
    }

    pub(super) fn take_response(&mut self) -> Option<Response> {
        self.1.take()
    }
}

/// A [`BoxFuture`] that represents a [`HandlerResponse`]
pub(crate) type HandlerFuture = BoxFuture<'static, HandlerResponse>;

pub(crate) trait HandlerInvoker: Send + Sync {
    fn descriptors(&self) -> Vec<MessageInvokerDescriptor>;
    fn create_invoker<'a>(&'a self, ctx: DispatchContext) -> Option<MessageHandlerInvoker<'a>>;
}
