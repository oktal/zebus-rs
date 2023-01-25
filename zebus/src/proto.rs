pub use crate::directory::descriptor::proto::PeerDescriptor;
pub use crate::message_id::proto::MessageId;
pub use crate::message_type_id::proto::MessageTypeId;
pub use crate::routing::binding_key::proto::BindingKey;
pub use crate::subscription::proto::Subscription;

pub mod prost {
    pub use zebus_proto::{DecodeError, EncodeError, Message};
    pub use zebus_proto_derive::{Enumeration, Message};
}

pub mod bcl {
    pub use crate::bcl::*;
}

/// A trait to turn a protobuf type into its Rust equivalent
pub(crate) trait FromProtobuf {
    /// Type to convert from
    type Input;

    fn from_protobuf(input: Self::Input) -> Self;
}

/// A trait to turn a Rust type into its protobuf equivalent
pub(crate) trait IntoProtobuf {
    /// Protobuf message type to convert into
    type Output;

    fn into_protobuf(self) -> Self::Output;
}

pub(crate) trait AsProtobuf {
    type Output;
    fn as_protobuf(&self) -> Self::Output;
}

impl<T> AsProtobuf for T
where
    T: IntoProtobuf + Clone,
{
    type Output = <T as IntoProtobuf>::Output;

    fn as_protobuf(&self) -> Self::Output {
        self.clone().into_protobuf()
    }
}

impl<T: FromProtobuf> FromProtobuf for Option<T> {
    type Input = Option<T::Input>;

    fn from_protobuf(input: Self::Input) -> Self {
        input.map(T::from_protobuf)
    }
}

impl<T: FromProtobuf> FromProtobuf for Vec<T> {
    type Input = Vec<T::Input>;

    fn from_protobuf(input: Self::Input) -> Self {
        input.into_iter().map(T::from_protobuf).collect()
    }
}

impl<T: IntoProtobuf> IntoProtobuf for Option<T> {
    type Output = Option<T::Output>;

    fn into_protobuf(self) -> Self::Output {
        self.map(|v| v.into_protobuf())
    }
}

impl<T: IntoProtobuf> IntoProtobuf for Vec<T> {
    type Output = Vec<T::Output>;

    fn into_protobuf(self) -> Self::Output {
        self.into_iter().map(|v| v.into_protobuf()).collect()
    }
}
