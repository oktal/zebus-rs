pub use crate::directory::descriptor::proto::PeerDescriptor;
pub use crate::message_id::proto::MessageId;
pub use crate::message_type_id::proto::MessageTypeId;
pub use crate::routing::binding_key::proto::BindingKey;
pub use crate::subscription::proto::Subscription;

pub mod bcl {
    pub use crate::bcl::*;
}

pub(crate) trait IntoProtobuf {
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
