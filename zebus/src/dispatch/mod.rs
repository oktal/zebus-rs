mod dispatcher;
mod future;
mod queue;
pub(crate) mod registry;

use crate::transport::TransportMessage;
use crate::MessageTypeDescriptor;
pub(crate) use dispatcher::{Error, MessageDispatcher};
pub use zebus_core::{DispatchHandler, Handler, HandlerError, DEFAULT_DISPATCH_QUEUE};

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
    UserError(i32, String),
    StandardError(Box<dyn std::error::Error + Send>),
}

impl DispatchResult {
    pub(self) fn from_response<R>(response: R) -> Self
    where
        R: crate::Message + prost::Message + 'static,
    {
        let message_type_descriptor = MessageTypeDescriptor::of::<R>();
        let payload = response.encode_to_vec();
        Self::Response(message_type_descriptor, payload)
    }

    pub(self) fn from_error<E>(error: HandlerError<E>) -> Self
    where
        E: crate::Error,
    {
        match error {
            crate::HandlerError::User(e) => Self::UserError(e.code(), e.to_string()),
            crate::HandlerError::Standard(e) => Self::StandardError(e),
        }
    }

    pub(self) fn is_error(&self) -> bool {
        matches!(self, Self::UserError(..) | Self::StandardError(..))
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
