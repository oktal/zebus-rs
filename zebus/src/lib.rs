mod bcl;
pub mod bus;
pub mod configuration;
pub mod core;
mod directory;
pub mod dispatch;
pub mod inject;
pub mod lotus;
mod message_id;
mod message_type_descriptor;
mod message_type_id;
mod peer;
mod peer_id;
mod persistence;
pub mod proto;
mod routing;
mod subscription;
mod sync;
pub mod transport;

pub use bus::{Bus, Command, CommandError, Event, Message, MessageExt, SendError};
pub use configuration::{BusConfiguration, ConfigurationProvider};

pub use crate::core::{BusBuilder, Error, HandlerError, IntoResponse, Response, ResponseMessage};

pub use message_id::MessageId;
pub use peer::Peer;
pub use peer_id::PeerId;
pub use subscription::Subscription;

use message_type_id::{MessageType, MessageTypeId};

pub use routing::BindingKey;
pub use zebus_core::{
    BindingExpression, BindingKeyFragment, BoxError, HandlerDescriptor, MessageDescriptor,
    MessageFlags, MessageKind, MessageTypeDescriptor, SubscriptionMode, Upcast,
    DEFAULT_DISPATCH_QUEUE,
};
pub use zebus_macros::{handler, Command, Event};

extern crate self as zebus;
