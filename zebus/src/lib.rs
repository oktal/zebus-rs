#![feature(downcast_unchecked)]
#![feature(map_try_insert)]

mod bcl;
mod bus;
mod bus_configuration;
mod core;
mod directory;
mod dispatch;
pub mod lotus;
mod message_id;
mod message_type_descriptor;
mod message_type_id;
mod peer;
mod peer_id;
pub mod proto;
mod routing;
mod subscription;
mod sync;
pub mod transport;

pub use bus::Bus;
pub use bus_configuration::BusConfiguration;

pub use crate::core::{
    BusBuilder, CreateError, Error, Handler, HandlerError, IntoResponse, Response, ResponseMessage,
};

pub use message_id::MessageId;
pub use peer::Peer;
pub use peer_id::PeerId;
pub use subscription::Subscription;

use message_type_descriptor::MessageTypeDescriptor;
use message_type_id::{MessageType, MessageTypeId};

pub use zebus_core::{
    BindingKeyFragment, Command, DispatchHandler, Event, Message, MessageBinding, MessageKind,
    DEFAULT_DISPATCH_QUEUE,
};
pub use zebus_macros::{Command, Event, Handler};

pub use routing::BindingKey;
