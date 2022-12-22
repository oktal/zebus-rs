use std::{
    any::{type_name, Any},
    collections::{hash_map::Entry, HashMap},
};

use super::{Dispatch, DispatchHandler, Handler, MessageDispatch};
use crate::{sync::LockCell, Message, MessageTypeDescriptor};

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
        M: Message + prost::Message + Default + 'static,
    {
        let invoker = |dispatch: &MessageDispatch, handler: &mut dyn Any| {
            if let Some(Ok(message)) = dispatch.message.decode_as::<M>() {
                // Safety:
                //   1. `handler` is of type `Box<H>
                //   2. `H` has an explicit bound on `Handler<T>`
                unsafe { handler.downcast_mut_unchecked::<H>() }.handle(message);
            }
        };

        let name = <M as Message>::name();
        match self.invokers.entry(name) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => Some(e.insert(MessageInvoker {
                descriptor: MessageTypeDescriptor::of::<M>(),
                invoker: Box::new(invoker),
            })),
        }
        .expect(&format!(
            "attempted to double-insert handler for {}",
            type_name::<M>()
        ));

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
}

impl<H> Dispatch for Registry<H>
where
    H: DispatchHandler + Send + 'static,
{
    fn dispatch(&mut self, dispatch: &MessageDispatch) {
        let message_type_id = &dispatch.message.message_type_id;

        if let Some(entry) = self.invokers.get_mut(message_type_id.full_name.as_str()) {
            self.handler.apply_mut(|h| {
                (entry.invoker)(dispatch, h);
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
