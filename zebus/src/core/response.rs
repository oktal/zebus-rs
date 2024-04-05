//! Response types for handled commands
use core::fmt;

use super::RawMessage;
use crate::{
    proto::IntoProtobuf, transport::MessageExecutionCompleted, BoxError, MessageDescriptor,
    MessageId, MessageTypeDescriptor,
};

/// Error code returned in [`MessageExecutionCompleted`] when a handler for a message returned a
/// generic [`std::error::Error`]
pub const HANDLER_ERROR_CODE: i32 = -10;

/// Error code returned when an internal error occured
pub const INTERNAL_ERROR: i32 = -20;

/// A protobuf [`prost::Message`] message returned by a message handler
pub struct ResponseMessage<T>(pub T);

impl<T> From<T> for ResponseMessage<T> {
    fn from(message: T) -> Self {
        Self(message)
    }
}

/// Response returned by a message handler
///
/// Command handlers can complete and return a result back to the originator of a
/// [`crate::Command`]
///
/// A response can be either:
/// - A succesfull response message that will be encoded as a protobuf message
/// - A user [`crate::Error`] error indicating a business error when handling a [`crate::Command`]
/// - A standard [`std::error::Error`] raised by common Rust faillible functions
#[derive(Debug)]
pub enum Response {
    /// A [`crate::Message`] response returned by a message handler.
    ///
    /// This contains the [`MessageTypeDescriptor`] of the message as well as the raw protobuf-encoded payload of the
    /// [`crate::Message`]
    Message(RawMessage),

    /// A business [`crate::Error`] error returned by a message handler.
    ///
    /// This contains the error code and the string representation of the error
    Error(i32, String),

    /// A standard Rust [`std::error::Error`] raised by common Rust faillible functions
    StandardError(BoxError),
}

impl Response {
    pub(crate) fn into_message(self, command_id: MessageId) -> MessageExecutionCompleted {
        let command_id = command_id.into_protobuf();
        match self {
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
        }
    }
}

/// Trait for generating responses
///
/// Types that implement this trait can be returned from a message handler
/// This trait is implemented for common types
pub trait IntoResponse {
    fn into_response(self) -> Option<Response>;
}

/// User error type that can be returned by a message handler
pub trait Error: std::error::Error {
    /// Numeric representation of the underlying error
    fn code(&self) -> i32;
}

/// Error type that can be returned by a message handler.
///
/// A handler can fail it two ways.
/// 1. Handling a message can succeed but yield a logical error
/// 2. Handling a message can fail by invoking a faillible operation that failed, e.g an operation
///    that yields a [`Result`] and failed with an `Err`.
///    This would be the equivalent of an exception in other languages.
#[derive(Debug)]
pub enum HandlerError<E: Error> {
    /// A standard [`Error`](std::error::Error).
    ///
    /// Corresponds to a faillible operation that failed
    Standard(BoxError),

    /// A user [`Error`].
    ///
    /// Corresponds to a logical error raised when handling a message
    User(E),
}

pub(crate) struct InternalError<E>(pub(crate) E)
where
    E: std::error::Error;

impl<E, Err> From<Err> for HandlerError<E>
where
    E: Error,
    Err: Into<BoxError>,
{
    fn from(error: Err) -> Self {
        Self::Standard(error.into())
    }
}

impl<E> fmt::Display for HandlerError<E>
where
    E: Error + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerError::Standard(e) => write!(f, "{}", e),
            HandlerError::User(e) => write!(f, "{}", e),
        }
    }
}

impl<E: Error> IntoResponse for HandlerError<E> {
    fn into_response(self) -> Option<Response> {
        Some(match self {
            HandlerError::Standard(e) => Response::StandardError(e),
            HandlerError::User(e) => Response::Error(e.code(), format!("{}", e)),
        })
    }
}

/// Turn an error into an `Error` [`Response`]
impl IntoResponse for BoxError {
    fn into_response(self) -> Option<Response> {
        Some(Response::StandardError(self))
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Option<Response> {
        None
    }
}

/// Turn a `ResponseMessage` into a `Message` [`Response`]
impl<T> IntoResponse for ResponseMessage<T>
where
    T: MessageDescriptor + prost::Message + 'static,
{
    fn into_response(self) -> Option<Response> {
        let message_type_descriptor = MessageTypeDescriptor::of::<T>();
        let payload = self.0.encode_to_vec();
        Some(Response::Message(RawMessage::new(
            message_type_descriptor,
            payload,
        )))
    }
}

/// Turn a `Result<T, E>` into a [`Response`]
impl<T, E> IntoResponse for Result<T, E>
where
    T: IntoResponse,
    E: IntoResponse,
{
    fn into_response(self) -> Option<Response> {
        match self {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        }
    }
}

/// Turn a `(i32, string)` into an `Error` [`Response`]
impl<E> IntoResponse for (i32, E)
where
    E: Into<String>,
{
    fn into_response(self) -> Option<Response> {
        Some(Response::Error(self.0, self.1.into()))
    }
}

/// Turn an [`Error`] into an `Error` [`Response`]
impl<E> IntoResponse for E
where
    E: Error,
{
    fn into_response(self) -> Option<Response> {
        Some(Response::Error(self.code(), format!("{}", self)))
    }
}

impl<E> IntoResponse for InternalError<E>
where
    E: std::error::Error,
{
    fn into_response(self) -> Option<Response> {
        Some(Response::Error(INTERNAL_ERROR, self.0.to_string()))
    }
}
