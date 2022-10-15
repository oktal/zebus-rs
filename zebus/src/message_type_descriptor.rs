use std::any::TypeId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MessageTypeDescriptor {
    /// Fully-qualified name of the message
    pub(crate) full_name: String,

    /// Rust type representation of the message
    pub(crate) r#type: TypeId,

    /// Marker flag for a persistent message
    pub(crate) is_persistent: bool,

    /// Market flag for an infrastructure message
    pub(crate) is_infrastructure: bool,
    // TODO(oktal): Handle routing fields info
}
