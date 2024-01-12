use zebus_core::MessageTypeDescriptor;

use crate::{message_type_id::proto, proto::IntoProtobuf};

impl IntoProtobuf for MessageTypeDescriptor {
    type Output = proto::MessageTypeId;

    fn into_protobuf(self) -> Self::Output {
        proto::MessageTypeId {
            full_name: self.full_name.to_string(),
        }
    }
}
