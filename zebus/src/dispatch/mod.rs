mod dispatcher;
mod future;
mod queue;
pub(crate) mod registry;

use crate::transport::TransportMessage;
use crate::MessageTypeDescriptor;
pub(crate) use dispatcher::{Error, MessageDispatcher};
pub use zebus_core::{DispatchHandler, Handler, DEFAULT_DISPATCH_QUEUE};

use self::future::DispatchFuture;

/// Represents a [`TransportMessage`] to dispatch to the corresponding [`Handler`]
pub(crate) struct MessageDispatch {
    pub message: TransportMessage,
    future: DispatchFuture,
}

impl MessageDispatch {
    pub(self) fn new(message: TransportMessage) -> (Self, DispatchFuture) {
        let dispatch = Self {
            message,
            future: DispatchFuture::new(),
        };
        let future = dispatch.future.clone();
        (dispatch, future)
    }

    pub(self) fn has_error(&self) -> bool {
        self.future.has_error()
    }

    pub(self) fn set_handled(&self, result: DispatchResult) {
        self.future.set_handled(result);
    }

    pub(self) fn set_completed(&self) {
        self.future.set_completed()
    }
}

#[derive(Debug)]
pub(crate) enum DispatchResult {
    Response(MessageTypeDescriptor, Vec<u8>),
    Error(i32, String),
}

impl DispatchResult {
    pub(self) fn is_error(&self) -> bool {
        matches!(self, Self::Error(..))
    }
}

impl DispatchResult {
    fn from_response<R>(response: R) -> Self
    where
        R: crate::Message + prost::Message + 'static,
    {
        let message_type_descriptor = MessageTypeDescriptor::of::<R>();
        let payload = response.encode_to_vec();
        Self::Response(message_type_descriptor, payload)
    }

    fn from_error(error: impl crate::Error) -> Self {
        Self::Error(error.code(), error.to_string())
    }
}

pub(crate) trait Dispatcher {
    fn dispatch(&mut self, message: TransportMessage) -> DispatchFuture;
}

pub(crate) trait Dispatch {
    fn dispatch(&mut self, dispatch: &MessageDispatch);
}

impl Dispatch for Vec<Box<dyn Dispatch + Send>> {
    fn dispatch(&mut self, dispatch: &MessageDispatch) {
        for d in self {
            d.dispatch(dispatch);

            // TODO(oktal): should we stop after the first error ?
            if dispatch.has_error() {
                return;
            }
        }
    }
}
