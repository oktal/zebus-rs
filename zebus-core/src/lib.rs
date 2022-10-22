use std::{fmt, marker::PhantomData};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingField {
    pub index: usize,
    pub routing_position: usize,
}

/// A fragment of a ['BindingKey`] that will represent either a string literal, a star `*` or a
/// sharp `#`
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingKeyFragment {
    Value(String),
    Star,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BindingKey {
    pub fragments: Option<Vec<BindingKeyFragment>>,
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

/// A message that can be sent on the bus.
/// Messages that are sent on the bus are either [`Command`] or [`Event`]
/// A [`Command`] is sent to a unique peer, asking for an action to be performed
/// An [`Event`] can be published to multiple peers, notifying that an action has been performed
pub trait Message {
    /// Marker flag for infrastructure messages
    const INFRASTRUCTURE: bool;

    /// Marker flag for non-persistent messages
    const TRANSIENT: bool;

    /// Namespace this messages belongs to
    fn name() -> &'static str;

    /// Fields on which this message can be routed
    fn routing() -> &'static [RoutingField];

    /// Get the the [`BindingKey`] for this message
    fn get_binding(&self) -> BindingKey;
}

pub trait MessageBinding {
    type Binding: Default;

    fn bind(binding: Self::Binding) -> BindingKey;
}

/// A message that can be sent to a peer, asking for an action to be performed
pub trait Command: Message {}

/// A message that can be published to multiple peers, notifying that an action has been performed
pub trait Event: Message {}

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
