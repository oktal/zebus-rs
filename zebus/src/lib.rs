mod bcl;
pub mod bus;
mod bus_configuration;
pub mod core;
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

pub use bus::{Bus, Command, CommandError, Event, Message, MessageExt, SendError};
pub use bus_configuration::BusConfiguration;

pub use crate::core::{
    BusBuilder, Context, CreateError, Error, Handler, HandlerError, IntoResponse, Response,
    ResponseMessage,
};

pub use message_id::MessageId;
pub use peer::Peer;
pub use peer_id::PeerId;
pub use subscription::Subscription;

use message_type_descriptor::MessageTypeDescriptor;
use message_type_id::{MessageType, MessageTypeId};

pub use zebus_core::{
    BindingKeyFragment, DispatchHandler, MessageBinding, MessageDescriptor, MessageFlags,
    MessageKind, SubscriptionMode, Upcast, DEFAULT_DISPATCH_QUEUE,
};
pub use zebus_macros::{handler, Command, Event, Handler};

pub use routing::BindingKey;
