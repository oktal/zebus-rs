mod dispatcher;
mod future;
mod queue;
pub(crate) mod registry;

use std::fmt::Display;

use crate::bus::HANDLER_ERROR_CODE;
use crate::core::RawMessage;
use crate::proto::FromProtobuf;
use crate::proto::IntoProtobuf;
use crate::transport::MessageExecutionCompleted;
use crate::transport::OriginatorInfo;
use crate::transport::TransportMessage;
use crate::MessageId;
use crate::MessageKind;
use crate::MessageTypeDescriptor;
use crate::Peer;
pub(crate) use dispatcher::{Error, MessageDispatcher};
pub use zebus_core::{DispatchHandler, Handler, HandlerError, DEFAULT_DISPATCH_QUEUE};

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

    pub(self) fn set_output(&self, output: DispatchOutput) {
        self.future.set_output(output);
    }

    pub(self) fn add_error(&self, error: ErrorRepr) {
        self.future.add_error(error);
    }

    pub(self) fn set_completed(self) {
        self.future.set_completed();
    }
}

/// Upon calling a user-specified [`Handler`] for a given [`TransportMessage`], the handler can
/// produce an output to be sent back to the originator of the message.
/// A [`Handler`] can either produce a [`Message`] response or an error with an error code and
/// string representation, that should be sent back to the originator into the final
/// [`MessageExecutionCompleted`]
#[derive(Debug)]
pub(crate) enum DispatchOutput {
    /// A [`Message`] response returned by a [`Handler`]. This contains the
    /// [`MessageTypeDescriptor`] of the message as well as the raw protobuf-encoded payload of the
    /// [`Message`]
    Response(RawMessage<MessageTypeDescriptor>),

    /// An [`Error`] returned by a [`Handler`]. This contains the error code and the string
    /// representation of the error
    Error(i32, String),
}

impl DispatchOutput {
    /// Returns `true` if the [`DispatchOutput`] contains an error
    fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Create a [`DispatchOutput`] with an associated [`Message`] `response`
    fn with_response<R>(response: R) -> Self
    where
        R: crate::Message + prost::Message + 'static,
    {
        let message_type_descriptor = MessageTypeDescriptor::of::<R>();
        let payload = response.encode_to_vec();
        Self::Response(RawMessage::new(message_type_descriptor, payload))
    }

    /// Create a [`DispatchOutput`] with an associated [`Error`] `error`
    fn with_error<E>(error: E) -> Self
    where
        E: crate::Error,
    {
        Self::Error(error.code(), error.to_string())
    }
}

type ErrorRepr = Box<dyn std::error::Error + Send>;

#[derive(Debug, Default)]
pub(crate) struct DispatchError(Vec<ErrorRepr>);

impl DispatchError {
    fn add(&mut self, error: ErrorRepr) {
        self.0.push(error);
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
pub(crate) type DispatchResult = Result<Option<DispatchOutput>, DispatchError>;

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
                DispatchOutput::Response(message) => {
                    let (message_type, payload) = message.into();
                    MessageExecutionCompleted {
                        command_id,
                        error_code: 0,
                        payload_type_id: Some(message_type.into_protobuf()),
                        payload: Some(payload),
                        response_message: None,
                    }
                }

                DispatchOutput::Error(error_code, message) => MessageExecutionCompleted {
                    command_id,
                    error_code,
                    payload_type_id: None,
                    payload: None,
                    response_message: Some(message),
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
                response_message: Some(e.to_string())
            }
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
