/// Identifies a named peer throughout bus communication between peers
#[derive(Clone, Eq, PartialEq, prost::Message)]
pub struct PeerId {
    #[prost(string, tag = "1")]
    value: String,
}

impl PeerId {
    pub fn new(value: String) -> Self {
        Self { value }
    }
}
