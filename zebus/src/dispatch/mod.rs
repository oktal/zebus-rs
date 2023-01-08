mod dispatcher;
mod future;
mod queue;
pub(crate) mod registry;

use std::fmt::Display;

use crate::core::{response::HANDLER_ERROR_CODE, Response};
use crate::lotus::MessageProcessingFailed;
use crate::proto::IntoProtobuf;
use crate::{
    transport::{MessageExecutionCompleted, TransportMessage}, MessageKind, Peer,
};
pub(crate) use dispatcher::{Error, MessageDispatcher};

use self::future::DispatchFuture;

/// A [`TransportMessage`] to be dispatched
pub(crate) struct MessageDispatch {
    message: TransportMessage,
    future: DispatchFuture,
}

impl MessageDispatch {
    pub(self) fn new(message: TransportMessage) -> (Self, DispatchFuture) {
        let dispatch = Self {
            message: message.clone(),
            future: DispatchFuture::new(message),
        };
        let future = dispatch.future.clone();
        (dispatch, future)
    }

    pub(self) fn set_kind(&self, kind: MessageKind) {
        self.future.set_kind(kind);
    }

    pub(self) fn set_response(&self, handler_type: &'static str, response: Option<Response>) {
        self.future.set_response(handler_type, response);
    }

    pub(self) fn set_completed(self) {
        self.future.set_completed();
    }
}

type ErrorRepr = (&'static str, Box<dyn std::error::Error + Send>);

#[derive(Debug, Default)]
pub(crate) struct DispatchError(Vec<ErrorRepr>);

impl DispatchError {
    fn add(&mut self, handler_type: &'static str, error: Box<dyn std::error::Error + Send>) {
        self.0.push((handler_type, error));
    }

    pub(crate) fn count(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, error) in self.0.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}: {}", error.0, error.1)?;
        }

        Ok(())
    }
}

/// A [`Result`] representation of a dispatch
pub(crate) type DispatchResult = Result<Option<Response>, DispatchError>;

/// Final representation of a [`TransportMessage`] that has been dispatched
#[derive(Debug)]
pub(crate) struct Dispatched {
    /// The [`TransportMessage`] that has been dispatched
    message: TransportMessage,

    /// [`MessageKind`] kind of message that has been dispatched
    kind: MessageKind,

    /// Result of the dispatch
    result: DispatchResult,
}

impl Dispatched {
    /// Returns `true` if the [`Dispatched`] contains an error
    pub(self) fn is_error(&self) -> bool {
        match self.result.as_ref() {
            Ok(output) => output.as_ref().map(|o| o.is_error()).unwrap_or(false),
            Err(_) => true,
        }
    }

    /// Returns `true` if a [`Command`] has been dispatched
    pub(crate) fn is_command(&self) -> bool {
        self.is(MessageKind::Command)
    }

    /// Returns `true` if an [`Event`] has been dispatched
    pub(crate) fn is_event(&self) -> bool {
        self.is(MessageKind::Event)
    }

    pub(crate) fn is(&self, kind: MessageKind) -> bool {
        matches!(self.kind, kind)
    }

    pub(crate) fn into_message(
        self,
    ) -> (
        Peer,
        Option<MessageExecutionCompleted>,
        Option<MessageProcessingFailed>,
    ) {
        let command_id = self.message.id.clone();
        let originator = self.message.originator.clone();
        let is_command = self.is_command();

        let originator = Peer {
            id: originator.sender_id,
            endpoint: originator.sender_endpoint,
            is_up: true,
            is_responding: true,
        };

        let message_procesing_failed = if let Err(dispatch_error) = self.result.as_ref() {
            let now_utc = chrono::Utc::now();
            let failing_handlers = dispatch_error.0.iter().map(|e| e.0.to_string()).collect();

            Some(MessageProcessingFailed {
                transport_message: self.message,
                // TODO(oktal): serialize message to JSON
                message_json: String::new(),
                exception_message: dispatch_error.to_string(),
                exception_timestamp_utc: now_utc.into_protobuf(),
                failing_handlers,
            })
        } else {
            None
        };

        let message_execution_completed = if is_command {
            Some(match self.result {
                Ok(Some(response)) => response.into_message(command_id),
                Ok(None) => MessageExecutionCompleted {
                    command_id,
                    error_code: 0,
                    payload_type_id: None,
                    payload: None,
                    response_message: None,
                },
                Err(e) => MessageExecutionCompleted {
                    command_id,
                    error_code: HANDLER_ERROR_CODE,
                    payload_type_id: None,
                    payload: None,
                    response_message: Some(e.to_string()),
                },
            })
        } else {
            None
        };

        (
            originator,
            message_execution_completed,
            message_procesing_failed,
        )
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
        }
    }
}
