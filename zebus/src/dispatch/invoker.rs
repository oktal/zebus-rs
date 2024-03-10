use std::convert::Infallible;
use std::sync::Arc;
use std::task::Poll;

use futures_core::future::BoxFuture;
use tower_service::Service;

use crate::{BindingKey, MessageTypeDescriptor, Response, SubscriptionMode};

use super::DispatchRequest;

/// A descriptor for an invoker for a specific type of message
#[derive(Debug, Clone)]
pub struct MessageInvokerDescriptor {
    /// The `type_name()` of the invoker
    pub invoker_type: &'static str,

    /// Name of the dispatch queue
    pub dispatch_queue: Option<&'static str>,

    /// Descriptor of the message type
    pub message: MessageTypeDescriptor,

    /// Mode of subscription for the message
    pub subscription_mode: SubscriptionMode,

    /// Subscription bindings for the message
    pub bindings: Vec<BindingKey>,
}

/// Represents a [`Response`] for a specific handler type of a specific message
pub(crate) struct HandlerResponse(&'static str, Option<Response>);

impl HandlerResponse {
    pub(crate) fn new(name: &'static str, response: Option<Response>) -> Self {
        Self(name, response)
    }

    pub(super) fn handler_type(&self) -> &'static str {
        self.0
    }

    pub(super) fn take_response(&mut self) -> Option<Response> {
        self.1.take()
    }
}

/// Request to invoke a [`MessageHandlerInvoker`]
#[derive(Clone)]
pub struct InvokeRequest {
    /// Original dispatch request
    pub(crate) dispatch: Arc<DispatchRequest>,
}

pub trait InvokerService:
    Service<
        InvokeRequest,
        Response = Option<Response>,
        Error = Infallible,
        Future = BoxFuture<'static, Result<Option<Response>, Infallible>>,
    > + Send
{
    fn descriptors(&self) -> Vec<MessageInvokerDescriptor>;

    fn poll_invoke<'a>(&'a self, req: &InvokeRequest)
        -> Poll<Option<&'a MessageInvokerDescriptor>>;
}

#[cfg(test)]
impl dyn InvokerService {
    pub(crate) fn invoke<M>(
        &mut self,
        peer: crate::Peer,
        environment: String,
        message: M,
        bus: Arc<dyn crate::Bus>,
    ) -> BoxFuture<'static, Result<Option<Response>, Infallible>>
    where
        M: crate::Message,
    {
        let request = InvokeRequest {
            dispatch: Arc::new(DispatchRequest::local(
                peer,
                environment,
                Arc::new(message),
                bus,
            )),
        };

        if let Poll::Ready(Some(_)) = self.poll_invoke(&request) {
            self.call(request)
        } else {
            Box::pin(async move { Ok(None) })
        }
    }
}
