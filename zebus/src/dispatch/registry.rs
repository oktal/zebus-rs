use std::{
    any::{type_name, Any},
    collections::{hash_map::Entry, HashMap},
};

use super::{Dispatch, DispatchHandler, Handler, MessageDispatch};
use crate::Message;

type Invoker = dyn Fn(&MessageDispatch, &mut (dyn Any + 'static)) + Send;

pub struct Registry<H>
where
    H: DispatchHandler + Send,
{
    handler: Box<H>,
    invokers: HashMap<&'static str, Box<Invoker>>,
}

impl<H> Registry<H>
where
    H: DispatchHandler + Send,
{
    fn new(handler: Box<H>) -> Self {
        Self {
            handler,
            invokers: HashMap::new(),
        }
    }

    pub fn handles<T>(&mut self) -> &mut Self
    where
        H: Handler<T> + 'static,
        T: Message + prost::Message + Default + 'static,
    {
        let invoker = |dispatch: &MessageDispatch, handler: &mut dyn Any| {
            if let Some(Ok(message)) = dispatch.message.decode_as::<T>() {
                // Safety:
                //   1. `handler` is of type `Box<H>
                //   2. `H` has an explicit bound on `Handler<T>`
                unsafe { handler.downcast_mut_unchecked::<H>() }.handle(message);
            }
        };

        let name = <T as Message>::name();
        match self.invokers.entry(name) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => Some(e.insert(Box::new(invoker))),
        }
        .expect(&format!(
            "attempted to double-insert handler for {}",
            type_name::<T>()
        ));

        self
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

        if let Some(invoker) = self.invokers.get_mut(message_type_id.full_name.as_str()) {
            invoker(dispatch, &mut self.handler);
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
