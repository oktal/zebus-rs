/// Identifies a named peer throughout bus communication between peers
#[derive(Clone, Eq, PartialEq, Hash, prost::Message)]
pub struct PeerId {
    #[prost(string, tag = "1")]
    value: String,
}

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn directory(instance_id: usize) -> Self {
        let value = format!("Abc.Zebus.Directory.{instance_id}");
        Self { value }
    }
}
