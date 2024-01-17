mod close;
mod configuration;
mod inbound;
mod outbound;
mod socket_options;
mod transport;

#[cfg(unix)]
mod poller;

use std::io;

pub use configuration::ZmqTransportConfiguration;
pub use socket_options::ZmqSocketOptions;
use thiserror::Error;
pub use transport::ZmqTransport;

use crate::PeerId;

use super::TransportMessage;

type MessageStream = crate::sync::stream::BroadcastStream<TransportMessage>;

/// Associated Error type with zmq
#[derive(Debug, Error)]
pub enum Error {
    /// Inbound error
    #[error("receive error {0}")]
    Inbound(inbound::Error),

    /// Outbound error
    #[error("send error {0}")]
    Outbound(outbound::Error),

    /// Attempted to comunicate with an unknown peer
    #[error("unknown peer {0}")]
    UnknownPeer(PeerId),

    /// A function call that returns an [`std::ffi::OsStr`] or [`std::ffi::OsString`] yield
    /// invalid UTF-8 sequence
    #[error("an invalid UTF-8 sequence was returned by an ffi function call")]
    InvalidUtf8,

    /// IO Error
    #[error("IO {0}")]
    Io(io::Error),

    /// Protobuf message encoding error
    #[error("error encoding protobuf message {0}")]
    Encode(prost::EncodeError),

    /// Protobuf message decoding error
    #[error("error decoding protobuf message {0}")]
    Decode(prost::DecodeError),

    /// An operation was attempted while the [`ZmqTransport`] was in an invalid state for the
    /// operation
    #[error("An operation was attempted while the transport was not in a valid state")]
    InvalidOperation,
}

pub type Result<T> = std::result::Result<T, Error>;
