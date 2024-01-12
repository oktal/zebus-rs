use std::fmt;

use crate::PeerId;

#[derive(Clone, Eq, PartialEq, prost::Message)]
pub struct Peer {
    #[prost(message, required, tag = "1")]
    pub id: PeerId,

    #[prost(string, required, tag = "2")]
    pub endpoint: String,

    #[prost(bool, required, tag = "3")]
    pub is_up: bool,

    #[prost(bool, required, tag = "4")]
    pub is_responding: bool,
}

impl Peer {
    #[cfg(test)]
    pub(crate) fn test() -> Self {
        Self {
            id: PeerId::test(),
            endpoint: "tcp://*:*".to_string(),
            is_up: true,
            is_responding: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_not_responding(mut self) -> Self {
        self.is_responding = false;
        self
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer({}, {})", self.id.value(), self.endpoint)?;
        Ok(())
    }
}
