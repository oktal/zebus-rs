pub(crate) mod command;
pub mod event;
mod service;
pub mod transport;

use std::time::Duration;

use once_cell::sync::Lazy;
use thiserror::Error;

use crate::{
    directory::{DirectoryReader, MessageBinding, PeerDescriptor},
    transport::TransportMessage,
    BoxError, Peer, PeerId,
};

use self::command::PersistMessageCommand;

#[derive(Debug, Error)]
pub enum PersistenceError {
    /// Underlying transport error
    // TODO(oktal): maybe should Transport error be generic over T::Err where T: Transport?
    #[error(transparent)]
    Transport(#[from] BoxError),

    /// Protobuf message decoding error
    #[error("error decoding protobuf message {0}")]
    Decode(#[from] prost::DecodeError),

    /// A `MessageReplayed` has been received from the persistence  with a different replay id
    #[error("attempting to replay a message with a conflicting replay id ({conflicting_id} != {replay_id})")]
    ConflictingReplay {
        replay_id: uuid::Uuid,
        conflicting_id: uuid::Uuid,
    },

    // TODO(oktal): improve error message
    #[error("invalid phase")]
    InvalidPhase,

    /// Persistence service is unreachable
    #[error("failed to reach persistence service after {0:?}")]
    Unreachable(Duration),

    /// Attemtping to send to an async channel failed
    #[error("failed to send message")]
    Send,

    /// Task failed to complete execution
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    /// An operation was attempted while the [`ZmqTransport`] was in an invalid state for the
    /// operation
    #[error("An operation was attempted while the transport was not in a valid state")]
    InvalidOperation,
}

impl From<crate::transport::future::SendError> for PersistenceError {
    fn from(_value: crate::transport::future::SendError) -> Self {
        Self::Send
    }
}

#[derive(Debug)]
pub struct PersistenceRequest {
    message: TransportMessage,
    peers: Vec<Peer>,
}

impl From<crate::transport::future::SendEntry> for PersistenceRequest {
    fn from(value: crate::transport::future::SendEntry) -> Self {
        let crate::transport::future::SendEntry { message, peers, .. } = value;
        Self { message, peers }
    }
}

impl PersistenceRequest {
    pub(crate) fn persistent_peer_ids(&self, directory: &dyn DirectoryReader) -> Vec<PeerId> {
        let is_persistent = self.message.is_persistent();

        if is_persistent {
            self.peers
                .iter()
                .filter(|p| {
                    directory
                        .get_peer(&p.id)
                        .map(|descriptor| descriptor.is_persistent)
                        .unwrap_or(false)
                })
                .map(|p| p.id.clone())
                .collect()
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum PersistenceEvent {
    ReplayStarted,
    SafetyStarted,
    Normal,
}

type MessageStream = crate::sync::stream::BroadcastStream<TransportMessage>;

const BINDING: Lazy<MessageBinding> = Lazy::new(|| MessageBinding::any::<PersistMessageCommand>());

/// Returns whether the given [`PeerDescriptor`] is a persistence peer
pub fn is_persistence_peer(descriptor: &PeerDescriptor) -> bool {
    descriptor.handles(&BINDING)
}
