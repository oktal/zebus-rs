use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use thiserror::Error;

use crate::{
    core::{MessagePayload, RawMessage},
    transport::MessageExecutionCompleted,
    Command, MessageType, Peer, PeerId, proto::prost,
};

/// An error which can be returned when sending a command
#[derive(Debug, Error)]
pub enum CommandError {
    /// Error raised by a remote peer command handler
    #[error("execution of command returned error {code} with message: {message:?}")]
    Command {
        /// Error code returned by the handler of the command
        code: i32,

        /// Optional error message returned by the handler of the command
        message: Option<String>,
    },

    /// A local error occured when attempting to receive the result of the command
    #[error("failed to receive command: {0}")]
    Receive(tokio::sync::oneshot::error::RecvError),
}

/// Execution result of a [`Command`]
pub type CommandResult = Result<Option<RawMessage<MessageType>>, CommandError>;

impl MessagePayload for CommandResult {
    fn message_type(&self) -> Option<&str> {
        match self {
            Ok(Some(message)) => message.message_type(),
            _ => None,
        }
    }

    fn content(&self) -> Option<&[u8]> {
        match self {
            Ok(Some(message)) => message.content(),
            _ => None,
        }
    }
}

impl From<MessageExecutionCompleted> for CommandResult {
    fn from(message: MessageExecutionCompleted) -> Self {
        if message.error_code != 0 {
            Err(CommandError::Command {
                code: message.error_code,
                message: message.response_message,
            })
        } else {
            let response = match (message.payload_type_id, message.payload) {
                (Some(message_type), Some(payload)) => Some(RawMessage::new(message_type, payload)),
                _ => None,
            };

            Ok(response)
        }
    }
}

pub struct CommandFuture(pub(crate) tokio::sync::oneshot::Receiver<CommandResult>);

impl Future for CommandFuture {
    type Output = CommandResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pin = Pin::new(&mut self.0);
        let result = ready!(pin.poll(cx));
        Poll::Ready(match result {
            Ok(r) => r,
            Err(e) => Err(CommandError::Receive(e)),
        })
    }
}

/// A Bus
pub trait Bus {
    /// Error type associated with the bus
    type Err: std::error::Error;

    /// Configure the bus with the provided [`PeerId`] `peer_id` and `environment`
    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err>;

    /// Start the bus
    fn start(&mut self) -> Result<(), Self::Err>;

    /// Stop the bus
    fn stop(&mut self) -> Result<(), Self::Err>;

    /// Send a [`Command`] to the handling [`Peer`]
    fn send<C: Command + prost::Message + Send + 'static>(
        &mut self,
        command: C,
    ) -> Result<CommandFuture, Self::Err>;

    /// Send a [`Command`] to a destination [`Peer`]
    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: C,
        peer: Peer,
    ) -> Result<CommandFuture, Self::Err>;
}
