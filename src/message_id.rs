use crate::bcl::Guid;

/// Id of a message that is sent on the bus
#[derive(Clone, Eq, PartialEq, prost::Message)]
pub struct MessageId {
    #[prost(message, required, tag = "1")]
    value: Guid,
}

impl From<uuid::Uuid> for MessageId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self { value: uuid.into() }
    }
}
