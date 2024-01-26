use std::time::Duration;

use chrono::Utc;
use futures_core::Future;
use thiserror::Error;

use tokio_stream::StreamExt;

use super::{
    commands::{RegisterPeerCommand, RegisterPeerResponse, UnregisterPeerCommand},
    PeerDescriptor,
};
use crate::{
    core::MessagePayload,
    proto::{FromProtobuf, IntoProtobuf},
    transport::{
        MessageExecutionCompleted, SendContext, Transport, TransportExt, TransportMessage,
    },
    BoxError, Peer, Subscription,
};

#[derive(Debug, Error)]
pub enum RegistrationError {
    /// Transport error
    #[error("an error occurred during a transport operation {0}")]
    Transport(BoxError),

    /// Failed to deserialize a [`TransportMessage`]
    #[error("error decoding transport message {0:?} {1}")]
    Decode(TransportMessage, prost::DecodeError),

    /// Failed to deserialize a [`MessageExecutionCompleted`]
    #[error("invalid response from directory {0:?} {1}")]
    InvalidResponse(MessageExecutionCompleted, prost::DecodeError),

    /// An unexpected message was received as a response
    #[error("received unexpected message from directory during registration {0:?}")]
    UnexpectedMessage(TransportMessage),

    #[error("timeout after {0:?}")]
    Timeout(Duration),

    #[error("the stream of transport messages has been closed")]
    Closed,
}

#[derive(Debug)]
pub struct Registration {
    pub(crate) pending: Vec<TransportMessage>,
    pub(crate) result: Result<RegisterPeerResponse, RegistrationError>,
}

impl Registration {
    fn new(
        pending: Vec<TransportMessage>,
        result: Result<RegisterPeerResponse, RegistrationError>,
    ) -> Self {
        Self { pending, result }
    }
}

/// Initiate a new registration to a peer directory
async fn try_register<T: Transport>(
    transport: &mut T,
    descriptor: PeerDescriptor,
    environment: String,
    directory_endpoint: Peer,
) -> Result<Registration, RegistrationError> {
    // Create `RegisterPeerCommand`
    let peer = descriptor.peer.clone();
    let register_command = RegisterPeerCommand {
        peer: descriptor.into_protobuf(),
    };
    let (_message_id, message) = TransportMessage::create(&peer, environment, &register_command);

    // Subscribe to transport messages stream
    let mut rcv_rx = transport
        .subscribe()
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    // Send `RegisterPeerCommand`
    transport
        .send(
            std::iter::once(directory_endpoint),
            message.clone(),
            SendContext::default(),
        )
        .map_err(|e| RegistrationError::Transport(e.into()))?
        .await
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    let mut pending = Vec::new();

    while let Some(message) = rcv_rx.next().await {
        if let Some(completed) = message.decode_as::<MessageExecutionCompleted>() {
            match completed {
                Ok(completed) => {
                    // TODO(oktal): check that the `source_command_id` is `message_id` and error
                    // otherwise
                    // We received the `RegisterPeerResponse`
                    if let Some(response) = completed.decode_as::<RegisterPeerResponse>() {
                        return Ok(Registration::new(
                            pending,
                            response.map_err(|e| RegistrationError::InvalidResponse(completed, e)),
                        ));
                    } else {
                        // We received a message other than `RegisterPeerResponse`,
                        // save it and keep waiting for the `RegisterPeerResponse`
                        pending.push(message);
                    }
                }
                // We failed to deserialize the `MessageExecutionCompleted`,
                Err(e) => {
                    return Ok(Registration::new(
                        pending,
                        Err(RegistrationError::Decode(message, e)),
                    ));
                }
            }
        }
    }

    // If we reach that point, this means the transport message reception stream has been
    // closed unexpectedly
    Err(RegistrationError::Closed)
}

async fn try_unregister<T: Transport>(
    transport: &mut T,
    directory: Peer,
) -> Result<(), RegistrationError> {
    // Retrieve the current peer
    let peer = transport
        .peer()
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    let utc_now = Utc::now();

    // Create `UnregisterPeerCommand`
    let unregister = UnregisterPeerCommand {
        peer_id: peer.id,
        endpoint: Some(peer.endpoint),
        timestamp: Some(utc_now.into_protobuf()),
    };
    //
    // Subscribe to transport messages stream
    let mut rcv_rx = transport
        .subscribe()
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    // Send unregister command to the directory
    let unregister_id = transport
        .send_one(directory, &unregister, SendContext::default())
        .await
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    // Wait for the response of the [`UnregisterPeerCommand`]
    while let Some(message) = rcv_rx.next().await {
        if let Some(completed) = message.decode_as::<MessageExecutionCompleted>() {
            match completed {
                Ok(completed) => {
                    if uuid::Uuid::from_protobuf(completed.command_id.value) == unregister_id {
                        return Ok(());
                    }
                }
                // We failed to deserialize the `MessageExecutionCompleted`,
                Err(e) => {
                    return Err(RegistrationError::Decode(message, e));
                }
            }
        }
    }

    // If we reach that point, this means the transport message reception stream has been
    // closed unexpectedly
    Err(RegistrationError::Closed)
}

async fn with_timeout<Fut, R>(timeout: Duration, fut: Fut) -> Result<R, RegistrationError>
where
    Fut: Future<Output = Result<R, RegistrationError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => Err(RegistrationError::Timeout(timeout)),
    }
}

/// Initiate a new registration to a peer directory with a timeout
pub(crate) async fn register<T: Transport>(
    transport: &mut T,
    descriptor: PeerDescriptor,
    environment: String,
    directory_endpoint: Peer,
    timeout: Duration,
) -> Result<Registration, RegistrationError> {
    with_timeout(
        timeout,
        try_register(transport, descriptor, environment, directory_endpoint),
    )
    .await
}

/// Unregister the peer
pub(crate) async fn unregister<T: Transport>(
    transport: &mut T,
    directory: Peer,
    timeout: Duration,
) -> Result<(), RegistrationError> {
    with_timeout(timeout, try_unregister(transport, directory)).await
}
