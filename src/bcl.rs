#[derive(Clone, Copy, Eq, PartialEq, prost::Message)]
pub(crate) struct Guid {
    #[prost(fixed64, tag = "1")]
    lo: u64,
    #[prost(fixed64, tag = "2")]
    hi: u64,
}

impl From<uuid::Uuid> for Guid {
    fn from(uuid: uuid::Uuid) -> Self {
        let (lo, hi) = uuid.as_u64_pair();
        Self { hi, lo }
    }
}
