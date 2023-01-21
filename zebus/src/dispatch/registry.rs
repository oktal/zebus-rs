use std::{
    any::{type_name, Any},
    collections::{hash_map::Entry, HashMap},
};

use super::{Dispatch, DispatchRequest, MessageDispatch};
use crate::{
    core::{Handler, IntoResponse, MessagePayload},
    sync::LockCell,
    DispatchHandler, Message, MessageKind, MessageTypeDescriptor, proto::prost,
};

type InvokerFn = dyn Fn(&MessageDispatch, &mut (dyn Any + 'static)) + Send;

pub struct MessageInvoker {
    descriptor: MessageTypeDescriptor,
    invoker: Box<InvokerFn>,
}

pub struct Registry<H>
where
    H: DispatchHandler + Send,
{
    handler: LockCell<Box<H>>,
    invokers: HashMap<&'static str, MessageInvoker>,
}

impl<H> Registry<H>
where
    H: DispatchHandler + Send,
{
    fn new(handler: Box<H>) -> Self {
        Self {
            handler: LockCell::new(handler),
            invokers: HashMap::new(),
        }
    }

    pub fn handles<M>(&mut self) -> &mut Self
    where
        H: Handler<M> + 'static,
        M: Message + prost::Message + Clone + Default + 'static,
    {
        let invoker = |dispatch: &MessageDispatch, handler: &mut dyn Any| {
            let handler_type = std::any::type_name::<H>();
            match &dispatch.request {
                DispatchRequest::Remote(message) => {
                    if let Some(Ok(message)) = message.decode_as::<M>() {
                        // Safety:
                        //   1. `handler` is of type `Box<H>
                        //   2. `H` has an explicit bound on `Handler<M>`
                        let res = unsafe { handler.downcast_mut_unchecked::<H>() }.handle(message);
                        dispatch.set_response(handler_type, res.into_response());
                    }
                }
                DispatchRequest::Local(_message_type, message) => {
                    if let Some(message) = message.downcast_ref::<M>() {
                        // Safety:
                        //   1. `handler` is of type `Box<H>
                        //   2. `H` has an explicit bound on `Handler<M>`
                        let res = unsafe { handler.downcast_mut_unchecked::<H>() }
                            .handle(message.clone());
                        dispatch.set_response(handler_type, res.into_response());
                    }
                }
            }

            dispatch.set_kind(MessageKind::Command);
        };

        self.add::<M>(|| Box::new(invoker));
        self
    }

    pub(crate) fn split_half(
        self,
        pred: impl Fn(&MessageTypeDescriptor) -> bool,
    ) -> (Option<Self>, Option<Self>) {
        let mut first = HashMap::new();
        let mut second = HashMap::new();

        for (k, v) in self.invokers {
            if pred(&v.descriptor) {
                second.insert(k, v);
            } else {
                first.insert(k, v);
            }
        }

        match (first.is_empty(), second.is_empty()) {
            (true, true) => (
                Some(Self {
                    handler: self.handler,
                    invokers: HashMap::new(),
                }),
                None,
            ),
            (false, true) => (
                Some(Self {
                    handler: self.handler,
                    invokers: first,
                }),
                None,
            ),
            (true, false) => (
                None,
                Some(Self {
                    handler: self.handler,
                    invokers: second,
                }),
            ),
            (false, false) => {
                let [handler0, handler1] = self.handler.into_shared::<2>();
                (
                    Some(Self {
                        handler: handler0,
                        invokers: first,
                    }),
                    Some(Self {
                        handler: handler1,
                        invokers: second,
                    }),
                )
            }
        }
    }

    pub(crate) fn handled_messages(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.invokers.keys().map(|k| *k)
    }

    fn add<M: Message + 'static>(&mut self, invoker_fn: impl FnOnce() -> Box<InvokerFn>) {
        let name = <M as Message>::name();
        match self.invokers.entry(name) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => Some(e.insert(MessageInvoker {
                descriptor: MessageTypeDescriptor::of::<M>(),
                invoker: Box::new(invoker_fn()),
            })),
        }
        .expect(&format!(
            "attempted to double-insert handler for {}",
            type_name::<M>()
        ));
    }
}

impl<H> Dispatch for Registry<H>
where
    H: DispatchHandler + Send + 'static,
{
    fn dispatch(&mut self, dispatch: &MessageDispatch) {
        let message_type = dispatch.request.message_type();
        if let Some(entry) = self.invokers.get_mut(message_type) {
            self.handler.apply_mut(|h| {
                (entry.invoker)(dispatch, h.as_mut() as &mut dyn Any);
            });
        }
    }
}

pub(crate) fn for_handler<H>(
    handler: Box<H>,
    registry_fn: impl FnOnce(&mut Registry<H>),
) -> Box<Registry<H>>
where
    H: DispatchHandler + Send + 'static,
{
    let mut registry = Box::new(Registry::new(handler));
    registry_fn(registry.as_mut());
    registry
}
