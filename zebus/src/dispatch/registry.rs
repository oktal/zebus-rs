use futures_util::FutureExt;
use std::{
    any::type_name,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

use super::{
    invoker::MessageHandlerInvoker, DispatchContext, DispatchMessage, HandlerFuture,
    HandlerInvoker, HandlerResponse, MessageInvokerDescriptor,
};
use crate::{
    core::{Handler, HandlerDescriptor, IntoResponse, MessagePayload},
    DispatchHandler, MessageDescriptor,
};

type InvokerFn<H> = dyn Fn(DispatchContext, Arc<Mutex<H>>) -> HandlerFuture + Send + Sync;

struct MessageInvoker<H> {
    descriptor: MessageInvokerDescriptor,
    invoker: Box<InvokerFn<H>>,
}

impl<H> MessageInvoker<H> {
    fn create(&self, ctx: DispatchContext, handler: Arc<Mutex<H>>) -> HandlerFuture {
        (self.invoker)(ctx, handler)
    }
}

/// A registry of [`Handler`] handlers
pub struct Registry<H>
where
    H: DispatchHandler + Send,
{
    handler: Arc<Mutex<H>>,
    invokers: HashMap<&'static str, MessageInvoker<H>>,
}

impl<H> Registry<H>
where
    H: DispatchHandler + Send,
{
    fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(Mutex::new(handler)),
            invokers: HashMap::new(),
        }
    }

    /// Declares a handler for a specific type of message [`M`]
    pub fn handles<M>(&mut self) -> &mut Self
    where
        H: Handler<M> + HandlerDescriptor<M> + 'static,
        M: MessageDescriptor + prost::Message + Clone + Default + 'static,
    {
        self.add::<M>(Box::new(|req, handler| {
            Self::invoker_for::<M>(req, handler)
        }));
        self
    }

    fn invoker_for<M>(ctx: DispatchContext, handler: Arc<Mutex<H>>) -> HandlerFuture
    where
        H: Handler<M> + HandlerDescriptor<M> + 'static,
        M: MessageDescriptor + prost::Message + Clone + Default + 'static,
    {
        async move {
            let response = match ctx.message() {
                DispatchMessage::Remote(message) => {
                    if let Some(Ok(message)) = message.decode_as::<M>() {
                        let mut handler = handler.lock().await;
                        handler
                            .handle(message, ctx.handler_context())
                            .await
                            .into_response()
                    } else {
                        None
                    }
                }
                DispatchMessage::Local(message) => {
                    if let Some(message) = message.downcast_ref::<M>() {
                        let mut handler = handler.lock().await;
                        handler
                            .handle(message.clone(), ctx.handler_context())
                            .await
                            .into_response()
                    } else {
                        None
                    }
                }
            };

            HandlerResponse::for_handler::<H>(response)
        }
        .boxed()
    }

    fn add<M>(&mut self, invoker_fn: Box<InvokerFn<H>>)
    where
        M: MessageDescriptor + 'static,
        H: HandlerDescriptor<M>,
    {
        // TODO(oktal): validate that the message is routable if we have bindings for it
        match self.invokers.entry(M::name()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => Some(e.insert(MessageInvoker {
                descriptor: MessageInvokerDescriptor::of::<H, M>(),
                invoker: invoker_fn,
            })),
        }
        .expect(&format!(
            "attempted to double-insert handler for {}",
            type_name::<M>()
        ));
    }
}

impl<H> HandlerInvoker for Registry<H>
where
    H: DispatchHandler + Send + 'static,
{
    fn descriptors(&self) -> Vec<MessageInvokerDescriptor> {
        self.invokers
            .values()
            .map(|i| i.descriptor.clone())
            .collect()
    }

    fn create_invoker(&self, ctx: DispatchContext) -> Option<MessageHandlerInvoker<'_>> {
        self.invokers
            .get(ctx.message_type())
            .map(|invoker| MessageHandlerInvoker {
                descriptor: &invoker.descriptor,
                invoker: invoker.create(ctx, Arc::clone(&self.handler)),
            })
    }
}

pub(crate) fn for_handler<H>(
    handler: H,
    registry_fn: impl FnOnce(&mut Registry<H>),
) -> Box<dyn HandlerInvoker>
where
    H: DispatchHandler + Send + 'static,
{
    let mut registry = Box::new(Registry::new(handler));
    registry_fn(registry.as_mut());
    registry
}
