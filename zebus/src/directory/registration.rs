use std::{
    future::Future,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use chrono::Utc;
use thiserror::Error;
use tokio::{runtime::Runtime, time::Timeout};

use super::{
    commands::{RegisterPeerCommand, RegisterPeerResponse},
    PeerDescriptor,
};
use crate::{
    core::MessagePayload,
    proto::IntoProtobuf,
    transport::{self, MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    Peer,
};

#[derive(Debug, Error)]
pub enum RegistrationError {
    /// Transport error
    #[error("an error occured during a transport operation {0}")]
    Transport(Box<dyn std::error::Error>),

    /// Failed to deserialize a [`TransportMessage`]
    #[error("error decoding transport message {0:?} {1}")]
    Decode(TransportMessage, prost::DecodeError),

    /// Failed to deserialize a [`MessageExecutionCompleted`]
    #[error("invalid response from directory {0:?} {1}")]
    InvalidResponse(MessageExecutionCompleted, prost::DecodeError),

    /// An unexpected message was received as a response
    #[error("received unexpected from directory {0:?}")]
    UnexpectedMessage(TransportMessage),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

#[derive(Debug)]
pub struct Registration {
    pub(crate) pending_messages: Vec<TransportMessage>,
    pub(crate) result: Result<RegisterPeerResponse, RegistrationError>,
}

impl Registration {
    fn new(
        pending_messages: Vec<TransportMessage>,
        result: Result<RegisterPeerResponse, RegistrationError>,
    ) -> Self {
        Self {
            pending_messages,
            result,
        }
    }
}

struct Inner {
    /// The [`RegisterPeerCommand`] original message id
    message_id: uuid::Uuid,

    /// The [`Receiver`] from which to receive transport messages
    receiver: NonNull<transport::Receiver>,

    /// List of messages that were received during registration
    pending_messages: Vec<TransportMessage>,
}

/// Provides a [`std::future::Future`] of a registration request to a directory
pub(crate) struct RegistrationFuture {
    inner: Option<Inner>,
}

impl RegistrationFuture {
    unsafe fn register<T: Transport>(
        transport: &mut T,
        receiver: &transport::Receiver,
        self_peer: Peer,
        environment: String,
        directory_endpoint: Peer,
    ) -> Result<Self, RegistrationError> {
        let utc_now = Utc::now();

        let descriptor = PeerDescriptor {
            peer: self_peer.clone(),
            subscriptions: vec![],
            is_persistent: false,
            timestamp_utc: Some(utc_now.into()),
            has_debugger_attached: Some(false),
        };
        let register_command = RegisterPeerCommand {
            peer: descriptor.into_protobuf(),
        };
        let (message_id, message) =
            TransportMessage::create(&self_peer, environment, &register_command);

        let receiver = NonNull::from(receiver);

        let registration = RegistrationFuture {
            inner: Some(Inner {
                message_id,
                receiver,
                pending_messages: vec![],
            }),
        };

        transport
            .send(
                std::iter::once(directory_endpoint),
                message.clone(),
                SendContext::default(),
            )
            .map_err(|e| RegistrationError::Transport(e.into()))?;
        Ok(registration)
    }

    pub fn with_timeout(self, timeout: Duration) -> Timeout<Self> {
        tokio::time::timeout(timeout, self)
    }
}

impl Future for RegistrationFuture {
    type Output = Registration;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (inner, res) = match self.inner.take() {
            Some(Inner {
                message_id,
                mut receiver,
                mut pending_messages,
                ..
            }) => {
                // Safety: this is safe  because
                // 1. We constructed our state from a reference, so we are guaranteed that the
                //    pointer is non-null
                // 2. The initial caller guarantees that the `receiver` will live long enough
                match unsafe { receiver.as_mut() }.poll_recv(cx) {
                    Poll::Ready(Some(transport_message)) => {
                        if let Some(message) =
                            transport_message.decode_as::<MessageExecutionCompleted>()
                        {
                            match message {
                                Ok(message_execution_completed) => {
                                    // TODO(oktal): check that the `source_command_id` is `message_id` and error
                                    // otherwise We received the `RegisterPeerResponse`, we can resolve the
                                    // future
                                    if let Some(response) = message_execution_completed
                                        .decode_as::<RegisterPeerResponse>()
                                    {
                                        (
                                            None,
                                            Poll::Ready(Registration::new(
                                                pending_messages,
                                                response.map_err(|e| {
                                                    RegistrationError::InvalidResponse(
                                                        message_execution_completed,
                                                        e,
                                                    )
                                                }),
                                            )),
                                        )
                                    } else {
                                        // We received a message other than `RegisterPeerResponse`,
                                        // save it and keep waiting for the `RegisterPeerResponse`
                                        pending_messages.push(transport_message);
                                        (
                                            Some(Inner {
                                                message_id,
                                                receiver,
                                                pending_messages,
                                            }),
                                            Poll::Pending,
                                        )
                                    }
                                }
                                // We failed to deserialize the `MessageExecutionCompleted`,
                                // resolve the future with an error
                                Err(e) => (
                                    None,
                                    Poll::Ready(Registration::new(
                                        pending_messages,
                                        Err(RegistrationError::Decode(transport_message, e)),
                                    )),
                                ),
                            }
                        } else {
                            // We received a message other than `MessageExecutionCompleted`, save
                            // it and keep waiting for the `RegisterPeerResponse`
                            pending_messages.push(transport_message);
                            (
                                Some(Inner {
                                    message_id,
                                    receiver,
                                    pending_messages,
                                }),
                                Poll::Pending,
                            )
                        }
                    }
                    // We did not receive yet, keep waiting
                    _ => (
                        Some(Inner {
                            message_id,
                            receiver,
                            pending_messages,
                        }),
                        Poll::Pending,
                    ),
                }
            }
            _ => panic!("attempted to poll already resolved future"),
        };

        self.inner = inner;
        res
    }
}

/// Initiate a new registration to a peer directory
/// # Safety
/// The caller must guarantee that the lifetime of the `receiver` exceeds the lifetime of the future
pub(crate) unsafe fn register<T: Transport>(
    transport: &mut T,
    receiver: &transport::Receiver,
    self_peer: Peer,
    environment: String,
    directory_peer: Peer,
) -> Result<RegistrationFuture, RegistrationError> {
    RegistrationFuture::register(transport, receiver, self_peer, environment, directory_peer)
}

pub(crate) fn block_on<T: Transport>(
    runtime: Arc<Runtime>,
    transport: &mut T,
    receiver: &transport::Receiver,
    self_peer: Peer,
    environment: String,
    directory_peer: Peer,
    timeout: Duration,
) -> Result<Registration, RegistrationError> {
    // Safety: since we are blocking on the future, we are guaranteed that the `receiver` will
    // live long enough
    let registration = unsafe {
        register(
            transport,
            receiver,
            self_peer.clone(),
            environment.clone(),
            directory_peer.clone(),
        )
    }?;

    match runtime.block_on(async { registration.with_timeout(timeout).await }) {
        Ok(registration) => match registration.result {
            Ok(_) => Ok(registration),
            Err(e) => Err(e),
        },
        Err(_) => Err(RegistrationError::Timeout(timeout)),
    }
}
