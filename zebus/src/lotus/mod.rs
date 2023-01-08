use crate::{proto::bcl, transport::TransportMessage};

/// [`crate::Event`] raised when a [`crate::Handler`] raised an error
#[derive(prost::Message, crate::Event)]
#[zebus(namespace = "Abc.Zebus")]
pub struct MessageProcessingFailed {
    /// The [`TransportMessage`] that triggered the error
    #[prost(message, required, tag = 1)]
    pub transport_message: TransportMessage,

    /// The JSON representation of the [`TransportMessage`] that triggered the error
    #[prost(string, required, tag = 2)]
    pub message_json: String,

    /// The error or exception message
    #[prost(string, required, tag = 3)]
    pub exception_message: String,

    /// The timestamp at which the error occured
    #[prost(message, required, tag = 4)]
    pub exception_timestamp_utc: bcl::DateTime,

    /// The types of handlers that triggered the error
    #[prost(string, repeated, tag = 5)]
    pub failing_handlers: Vec<String>,
}
