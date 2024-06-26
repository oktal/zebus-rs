//! Core bus components
pub mod builder;
mod bus;
mod message;
pub mod response;

pub use builder::BusBuilder;
pub use message::{MessagePayload, RawMessage};
pub use response::{
    Error, HandlerError, IntoResponse, Response, ResponseMessage, HANDLER_ERROR_CODE,
};

pub use zebus_core::{
    Command, Event, HandlerDescriptor, Message, MessageDescriptor, MessageFlags, SubscriptionMode,
    Upcast, UpcastFrom,
};
