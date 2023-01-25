use crate::proto::{FromProtobuf, IntoProtobuf};

pub(crate) mod proto {
    use crate::{bcl::Guid, proto::prost};

    #[derive(Copy, Clone, Eq, PartialEq, prost::Message)]
    pub struct MessageId {
        #[prost(message, required, tag = "1")]
        pub value: Guid,
    }
}

/// Id of a message that is sent on the bus
#[derive(Debug)]
pub struct MessageId(uuid::Uuid);

impl MessageId {
    pub fn value(&self) -> uuid::Uuid {
        self.0
    }
}

impl From<uuid::Uuid> for MessageId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl FromProtobuf for MessageId {
    type Input = proto::MessageId;

    fn from_protobuf(input: Self::Input) -> Self {
        Self(uuid::Uuid::from_protobuf(input.value))
    }
}

impl IntoProtobuf for MessageId {
    type Output = proto::MessageId;

    fn into_protobuf(self) -> Self::Output {
        proto::MessageId {
            value: self.0.into(),
        }
    }
}
