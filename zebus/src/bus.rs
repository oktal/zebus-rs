use core::fmt;
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_trait::async_trait;
use dyn_clone::DynClone;
use thiserror::Error;
use tokio::task::JoinError;

use crate::{
    core::{MessageDescriptor, MessageFlags, MessagePayload, RawMessage, Upcast, UpcastFrom},
    directory, dispatch,
    transport::MessageExecutionCompleted,
    BoxError, MessageType, Peer, PeerId,
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
pub type CommandResult = std::result::Result<Option<RawMessage<MessageType>>, CommandError>;

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

/// Error raised when failing to register with the directory
#[derive(Debug)]
pub struct RegistrationError {
    inner: Vec<(Peer, directory::RegistrationError)>,
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "failed to register to directory:")?;
        for failure in &self.inner {
            writeln!(f, "    tried {}: {}", failure.0, failure.1)?;
        }

        Ok(())
    }
}

impl RegistrationError {
    pub(crate) fn new() -> Self {
        Self { inner: vec![] }
    }

    pub(crate) fn add(&mut self, peer: Peer, error: directory::RegistrationError) {
        self.inner.push((peer, error))
    }

    #[cfg(test)]
    pub(crate) fn find(
        &self,
        predicate: impl Fn(&directory::RegistrationError) -> bool,
    ) -> Option<&directory::RegistrationError> {
        self.inner.iter().find(|e| predicate(&e.1)).map(|x| &x.1)
    }
}

/// Error raised when failing to send a [`Message`] through the bus
#[derive(Debug, Error)]
pub enum SendError {
    /// An attempt to send a [`Command`] resulted in no candidat peer
    #[error("unable to find peer for command")]
    NoPeer,

    /// An attempt to send a [`Command`] resulted in multiple candidate peers
    #[error("can not send a command to multiple peers: {0:?}")]
    MultiplePeers(Vec<Peer>),

    /// A [`Command`] could not be send to a non responding [`Peer`]
    #[error("can not send a transient message to non responding peer {0:?}")]
    PeerNotResponding(Peer),

    /// The sender has been closed
    #[error("sender has been closed")]
    Closed,
}

/// Represents an error that can be raised by the [`Bus`]
#[derive(Debug, Error)]
pub enum Error {
    /// Transport error
    #[error("an error occured during a transport operation {0}")]
    Transport(BoxError),

    /// None of the directories tried for registration succeeded
    #[error("{0}")]
    Registration(RegistrationError),

    /// An error occured when sending a message to one or multiple peers
    #[error("{0}")]
    Send(SendError),

    /// An error occured when attempting to dispatch a message
    #[error("an error occured on the dispatcher {0}")]
    Dispatch(dispatch::Error),

    #[error("error waiting for task to terminate: {0}")]
    Join(JoinError),

    /// An operation was attempted while the [`Bus`] was in an invalid state
    #[error("an operation was attempted while the bus was not in a valid state")]
    InvalidOperation,
}

/// A wrapper arround a [`std::result::Result`] type for bus-specific [`Error`]
pub type Result<T> = std::result::Result<T, Error>;

pub trait EncodableMessage {
    fn encode_to_vec(&self) -> Vec<u8>;
}

impl<M: prost::Message> EncodableMessage for M {
    fn encode_to_vec(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

/// Trait for a message that can be sent through the bus
pub trait Message:
    crate::core::Message
    + EncodableMessage
    + DynClone
    + Send
    + Sync
    + std::fmt::Debug
    + Upcast<dyn crate::core::Message>
    + 'static
{
}

/// Trait for a message that can be sent to a peer, asking for an action to be performed
pub trait Command: Message + crate::core::Command + Upcast<dyn Message> {}

/// Trait for a message that can be published to multiple peers, notifying that an action has been performed
pub trait Event: Message + crate::core::Event + Upcast<dyn Message> {}

/// Extension trait for [`Message`]
pub trait MessageExt: Message {
    /// Returns `true` if the `Message` contains a [`MessageFlags`]
    fn has_flag(&self, flag: MessageFlags) -> bool {
        self.flags().contains(flag)
    }

    /// Returns `true` if the `Message` is flagged as an infrastructure message
    fn is_infrastructure(&self) -> bool {
        self.has_flag(MessageFlags::INFRASTRUCTURE)
    }

    /// Returns `true` if the `Message` is flagged as transient
    fn is_transient(&self) -> bool {
        self.has_flag(MessageFlags::TRANSIENT)
    }

    /// Returns `true` if the `Message` is persistent
    fn is_persistent(&self) -> bool {
        !self.is_transient()
    }
}

impl<M: Message + ?Sized> MessageExt for M {}

impl dyn Message {
    /// Returns `true` if the inner [`Message`] is of type `M`
    /// This function implements semantic similar to [`std::any::Any::is`]
    pub fn is<M: MessageDescriptor>(&self) -> bool {
        self.name() == M::name()
    }

    /// Returns some reference to the concrete `M` message type if this [`Message`] is of type `M`
    /// This function implements semantic similar to [`std::any::Any::downcast_ref`]
    pub fn downcast_ref<M: MessageDescriptor>(&self) -> Option<&M> {
        // Safety: we guarantee that the `Message` is of type `M` by checking prior to calling
        // `downcast_ref_unchecked`
        self.is::<M>()
            .then_some(unsafe { self.downcast_ref_unchecked() })
    }

    /// Returns some reference the concrete `M` message type if this [`Message`] is of type `M`
    /// This function implements semantic similar to [`std::any::Any::downcast_ref_unchecked`]
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the [`Message`] is of type `M`
    pub unsafe fn downcast_ref_unchecked<M: MessageDescriptor>(&self) -> &M {
        // Safety: the caller *MUST* guarantee that the `Message` is of type `M`
        unsafe { &*(self as *const dyn Message as *const M) }
    }
}

impl<'a, M: Message + 'a> UpcastFrom<M> for dyn Message + 'a {
    fn up_from(value: &M) -> &Self {
        value
    }
    fn up_from_mut(value: &mut M) -> &mut Self {
        value
    }
}

impl<T: Any + crate::core::Message + prost::Message + DynClone + Send + Sync> Message for T {}
impl<T: Message + crate::core::Command> Command for T {}
impl<T: Message + crate::core::Event> Event for T {}

/// A Bus
#[async_trait]
pub trait Bus: Send + Sync + 'static {
    /// Configure the bus with the provided [`PeerId`] `peer_id` and `environment`
    fn configure(&self, peer_id: PeerId, environment: String) -> Result<()>;

    /// Start the bus
    async fn start(&self) -> Result<()>;

    /// Stop the bus
    async fn stop(&self) -> Result<()>;

    /// Send a [`Command`] to the handling [`Peer`]
    async fn send(&self, command: &dyn Command) -> Result<CommandFuture>;

    /// Send a [`Command`] to a destination [`Peer`]
    async fn send_to(&self, command: &dyn Command, peer: Peer) -> Result<CommandFuture>;

    /// Send an [`Event`] to the handling [`Peer`] peers
    async fn publish(&self, event: &dyn Event) -> Result<()>;
}

/// A [`crate::Bus`] that does nothing
pub(crate) struct NoopBus;

#[async_trait]
impl Bus for NoopBus {
    fn configure(&self, _peer_id: PeerId, _environment: String) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn start(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn stop(&self) -> Result<()> {
        Err(Error::InvalidOperation)
    }

    async fn send(&self, _command: &dyn Command) -> Result<CommandFuture> {
        Err(Error::InvalidOperation)
    }

    async fn send_to(&self, _command: &dyn Command, _peer: Peer) -> Result<CommandFuture> {
        Err(Error::InvalidOperation)
    }

    async fn publish(&self, _event: &dyn Event) -> Result<()> {
        Err(Error::InvalidOperation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const _BUS_IS_OBJECT_SAFE: Option<&dyn Bus> = None;
}
