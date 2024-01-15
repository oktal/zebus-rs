use core::fmt;

/// Identifies a named peer throughout bus communication between peers
#[derive(Clone, Eq, PartialEq, Hash, prost::Message)]
pub struct PeerId {
    #[prost(string, tag = 1)]
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

    pub fn is_instance_of(&self, service_name: &str) -> bool {
        self.value.starts_with(service_name)
    }

    pub fn is_persistence(&self) -> bool {
        self.is_instance_of("Abc.Zebus.PersistenceService")
    }

    pub fn is_directory(&self) -> bool {
        self.is_instance_of("Abc.Zebus.Directory")
    }

    pub(crate) fn directory(instance_id: usize) -> Self {
        let value = format!("Abc.Zebus.Directory.{instance_id}");
        Self { value }
    }

    #[cfg(test)]
    pub(crate) fn test() -> Self {
        let id = uuid::Uuid::new_v4();
        Self::new(format!("Peer.Test.{id}"))
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
