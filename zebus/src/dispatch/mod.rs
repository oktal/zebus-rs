mod dispatcher;
mod queue;
pub(crate) mod registry;

use crate::transport::TransportMessage;
pub(crate) use dispatcher::{Error, MessageDispatcher};
pub use zebus_core::{DispatchHandler, Handler, DEFAULT_DISPATCH_QUEUE};

/// Represents a [`TransportMessage`] to dispatch to the corresponding [`Handler`]
pub(crate) struct MessageDispatch {
    pub(crate) message: TransportMessage,
}

impl MessageDispatch {
    pub(crate) fn for_message(message: TransportMessage) -> Self {
        Self { message }
    }
}

pub(crate) trait Dispatcher {
    fn dispatch(&mut self, message: TransportMessage);
}

pub(crate) trait Dispatch {
    fn dispatch(&mut self, dispatch: &MessageDispatch);
}
