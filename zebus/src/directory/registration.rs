use std::time::Duration;

use chrono::Utc;
use thiserror::Error;

use tokio_stream::StreamExt;

use super::{
    commands::{RegisterPeerCommand, RegisterPeerResponse},
    PeerDescriptor,
};
use crate::{
    core::MessagePayload,
    proto::IntoProtobuf,
    transport::{MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    Peer, Subscription,
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

    #[error("the stream of transport messages has been closed")]
    Closed,
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

/// Initiate a new registration to a peer directory
async fn try_register<T: Transport>(
    transport: &mut T,
    self_peer: Peer,
    subscriptions: Vec<Subscription>,
    environment: String,
    directory_endpoint: Peer,
) -> Result<Registration, RegistrationError> {
    // Create `RegisterPeerCommand`
    let utc_now = Utc::now();

    let descriptor = PeerDescriptor {
        peer: self_peer.clone(),
        subscriptions,
        is_persistent: false,
        timestamp_utc: Some(utc_now.into()),
        has_debugger_attached: Some(false),
    };
    let register_command = RegisterPeerCommand {
        peer: descriptor.into_protobuf(),
    };
    let (_message_id, message) =
        TransportMessage::create(&self_peer, environment, &register_command);

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
        .map_err(|e| RegistrationError::Transport(e.into()))?;

    let mut pending_messages = Vec::new();

    while let Some(message) = rcv_rx.next().await {
        if let Some(completed) = message.decode_as::<MessageExecutionCompleted>() {
            match completed {
                Ok(completed) => {
                    // TODO(oktal): check that the `source_command_id` is `message_id` and error
                    // otherwise
                    // We received the `RegisterPeerResponse`
                    if let Some(response) = completed.decode_as::<RegisterPeerResponse>() {
                        return Ok(Registration::new(
                            pending_messages,
                            response.map_err(|e| RegistrationError::InvalidResponse(completed, e)),
                        ));
                    } else {
                        // We received a message other than `RegisterPeerResponse`,
                        // save it and keep waiting for the `RegisterPeerResponse`
                        pending_messages.push(message);
                    }
                }
                // We failed to deserialize the `MessageExecutionCompleted`,
                Err(e) => {
                    return Ok(Registration::new(
                        pending_messages,
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

/// Initiate a new registration to a peer directory with a timeout
pub(crate) async fn register<T: Transport>(
    transport: &mut T,
    self_peer: Peer,
    subscriptions: Vec<Subscription>,
    environment: String,
    directory_endpoint: Peer,
    timeout: Duration,
) -> Result<Registration, RegistrationError> {
    let future = try_register(
        transport,
        self_peer,
        subscriptions,
        environment,
        directory_endpoint,
    );
    match tokio::time::timeout(timeout, future).await {
        Ok(registration) => registration,
        Err(_) => Err(RegistrationError::Timeout(timeout)),
    }
}
