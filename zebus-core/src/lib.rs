use bitflags::bitflags;
use std::{any::Any, fmt, marker::PhantomData, str::FromStr};

mod upcast;
pub use upcast::{Upcast, UpcastFrom};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingField {
    pub index: usize,
    pub routing_position: usize,
}

/// A fragment of a [`BindingKey`]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum BindingKeyFragment {
    /// Raw string literal
    Value(String),

    /// Star `*`
    Star,

    /// Sharp `#`
    Sharp,
}

impl BindingKeyFragment {
    pub fn is_star(&self) -> bool {
        matches!(self, BindingKeyFragment::Star)
    }

    pub fn is_sharp(&self) -> bool {
        matches!(self, BindingKeyFragment::Sharp)
    }
}

impl fmt::Display for BindingKeyFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindingKeyFragment::Value(s) => write!(f, "{s}"),
            BindingKeyFragment::Star => write!(f, "*"),
            BindingKeyFragment::Sharp => write!(f, "#"),
        }
    }
}

impl FromStr for BindingKeyFragment {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "*" => BindingKeyFragment::Star,
            "#" => BindingKeyFragment::Sharp,
            _ => BindingKeyFragment::Value(s.to_string()),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct BindingKey {
    pub fragments: Option<Vec<BindingKeyFragment>>,
}

impl BindingKey {
    pub fn from_raw_parts(fragments: Vec<BindingKeyFragment>) -> Self {
        Self {
            fragments: Some(fragments),
        }
    }
}

impl FromStr for BindingKey {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fragments: Result<Vec<_>, _> = s.split('.').map(FromStr::from_str).collect();

        Ok(Self {
            fragments: Some(fragments?),
        })
    }
}

impl From<Vec<String>> for BindingKey {
    fn from(value: Vec<String>) -> Self {
        let fragments = value.into_iter().map(BindingKeyFragment::Value).collect();
        Self {
            fragments: Some(fragments),
        }
    }
}

pub struct Binding<T> {
    fragment: BindingKeyFragment,
    _phantom: PhantomData<T>,
}

impl<T: ToString> Binding<T> {
    pub fn matches(&mut self, value: T) {
        self.fragment = BindingKeyFragment::Value(value.to_string());
    }

    pub fn any(&mut self) {
        self.fragment = BindingKeyFragment::Star;
    }

    pub fn bind(self) -> BindingKeyFragment {
        self.fragment
    }
}

impl<T> Default for Binding<T> {
    fn default() -> Self {
        Self {
            fragment: BindingKeyFragment::Star,
            _phantom: PhantomData,
        }
    }
}

bitflags! {
    /// Marker flags for a [`Message`]
    pub struct MessageFlags: u32 {
        /// None
        const NONE           = 0b00000000;

        /// Marker flag for infrastructure messages
        const INFRASTRUCTURE = 0b00000001;

        /// Marker flag for non-persistent messages
        const TRANSIENT      = 0b00000010;
    }
}

/// Descriptor for a [`Message`]
pub trait MessageDescriptor {
    /// Get the [`MessageKind`] of this message
    fn kind() -> MessageKind;

    /// Get the flags for this message
    fn flags() -> MessageFlags;

    /// Get the fully qualified name of the message
    fn name() -> &'static str;

    /// Get the routing fields of the message
    fn routing() -> &'static [RoutingField];
}

/// A message that can be sent on the bus.
/// Messages that are sent on the bus are either [`Command`] or [`Event`]
/// A [`Command`] is sent to a unique peer, asking for an action to be performed
/// An [`Event`] can be published to multiple peers, notifying that an action has been performed
pub trait Message: Any {
    /// Get the [`MessageKind`] of this message
    fn kind(&self) -> MessageKind;

    /// Get the flags for this message
    fn flags(&self) -> MessageFlags;

    /// Get the fully qualified name of the message
    fn name(&self) -> &'static str;

    /// Get the the [`BindingKey`] for this message
    fn get_binding(&self) -> BindingKey;

    fn as_any(&self) -> &dyn Any;
}

pub trait MessageBinding {
    type Binding: Default;

    fn bind(binding: Self::Binding) -> BindingKey;
}

/// Trait for a message that can be sent to a peer, asking for an action to be performed
pub trait Command: Message + Upcast<dyn Message> {}

/// Trait for a message that can be published to multiple peers, notifying that an action has been performed
pub trait Event: Message + Upcast<dyn Message> {}

impl<'a, M: Message + 'a> UpcastFrom<M> for dyn Message + 'a {
    fn up_from(value: &M) -> &Self {
        value
    }
    fn up_from_mut(value: &mut M) -> &mut Self {
        value
    }
}

/// Enum representing the type of a [`Message`]
/// A [`Message`] can either be a [`Command`] command or [`Event`] event
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// Message of type [`Command`]
    Command,

    /// Message of type [`Event`]
    Event,
}

#[macro_export]
macro_rules! fragment {
    (*) => {
        zebus_core::BindingKeyFragment::Star
    };

    (#) => {
        zebus_core::BindingKeyFragment::Sharp
    };

    ($lit:literal) => {
        zebus_core::BindingKeyFragment::Value($lit.to_string())
    };
}

#[macro_export]
macro_rules! binding_key {
    () => {
        zebus_core::BindingKey::default()
    };

    ($($x:tt),*) => {
        zebus_core::BindingKey {
            fragments: Some(vec![$(::zebus_core::fragment![$x]),+])
        }
    };
}

/// Specifies the startup subscription mode for a message handler
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SubscriptionMode {
    /// A [`Subscription`] for the message handler will automatically be performed on startup
    Auto,

    /// The subscription for the message handler must be manually performed through the
    /// [`bus::subscribe`)[crate::Bus::subscribe] function
    Manual,
}

/// Descriptor for a [`DispatchHandler`]
pub trait HandlerDescriptor<T> {
    /// The startup subscription mode for the message handler
    fn subscription_mode() -> SubscriptionMode;

    /// Get the binding keys for automatic subscription mode on startup for the message handler
    fn bindings() -> Vec<BindingKey>;
}

/// Name of the default dispatch queue
pub const DEFAULT_DISPATCH_QUEUE: &'static str = "DefaultQueue";

/// Represents a [`Handler`] that can be called in the context of a dispatch queue
pub trait DispatchHandler {
    const DISPATCH_QUEUE: &'static str;
}
