mod dispatcher;
mod future;
mod queue;
pub(crate) mod registry;

use std::fmt::Display;

use crate::core::{response::HANDLER_ERROR_CODE, RawMessage, Response};
use crate::proto::{FromProtobuf, IntoProtobuf};
use crate::transport::{MessageExecutionCompleted, OriginatorInfo};
use crate::Peer;
use crate::{transport::TransportMessage, MessageId};
use crate::{MessageKind, MessageTypeDescriptor};
pub(crate) use dispatcher::{Error, MessageDispatcher};

use self::future::DispatchFuture;

/// A [`TransportMessage`] to be dispatched
pub(crate) struct MessageDispatch {
    message: TransportMessage,
    future: DispatchFuture,
}

impl MessageDispatch {
    pub(self) fn new(message: TransportMessage) -> (Self, DispatchFuture) {
        let message_id = MessageId::from_protobuf(message.id.clone());
        let originator = message.originator.clone();

        let dispatch = Self {
            message,
            future: DispatchFuture::new(message_id, originator),
        };
        let future = dispatch.future.clone();
        (dispatch, future)
    }

    pub(self) fn set_kind(&self, kind: MessageKind) {
        self.future.set_kind(kind);
    }

    pub(self) fn set_response(&self, response: Option<Response>) {
        self.future.set_response(response);
    }

    pub(self) fn set_completed(self) {
        self.future.set_completed();
    }
}

type ErrorRepr = Box<dyn std::error::Error + Send>;

#[derive(Debug, Default)]
pub(crate) struct DispatchError(Vec<ErrorRepr>);

impl DispatchError {
    fn add(&mut self, error: ErrorRepr) {
        self.0.push(error);
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

            write!(f, "{}", error)?;
        }

        Ok(())
    }
}

/// A [`Result`] representation of a dispatch
pub(crate) type DispatchResult = Result<Option<Response>, DispatchError>;

/// Final representation of a [`TransportMessage`] that has been dispatched
#[derive(Debug)]
pub(crate) struct Dispatched {
    /// [`MessageId`] of the [`TransportMessage`]
    message_id: MessageId,

    /// Originator of the [`TransportMessage`]
    originator: OriginatorInfo,

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

    /// Turn the [`Dispatched`] into the [`TransportMessage`] response that can be sent back to the
    // [`Peer`] sender of the original [`TransportMessage`] message that has been dispatched
    pub(crate) fn into_transport(
        self,
        peer: &Peer,
        environment: String,
    ) -> (TransportMessage, Peer) {
        let command_id = self.message_id.into_protobuf();
        let message = match self.result {
            Ok(Some(output)) => match output {
                Response::Message(message) => {
                    let (message_type, payload) = message.into();
                    MessageExecutionCompleted {
                        command_id,
                        error_code: 0,
                        payload_type_id: Some(message_type.into_protobuf()),
                        payload: Some(payload),
                        response_message: None,
                    }
                }
                Response::Error(error_code, message) => MessageExecutionCompleted {
                    command_id,
                    error_code,
                    payload_type_id: None,
                    payload: None,
                    response_message: Some(message),
                },
                Response::StandardError(e) => MessageExecutionCompleted {
                    command_id,
                    error_code: HANDLER_ERROR_CODE,
                    payload_type_id: None,
                    payload: None,
                    response_message: Some(e.to_string()),
                },
            },
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
        };

        let (_, transport_message) = TransportMessage::create(peer, environment, &message);
        let sender = Peer {
            id: self.originator.sender_id,
            endpoint: self.originator.sender_endpoint,
            is_up: true,
            is_responding: true,
        };

        (transport_message, sender)
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
