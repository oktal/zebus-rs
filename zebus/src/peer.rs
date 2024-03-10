use std::fmt;

use crate::{transport::OriginatorInfo, PeerId};

#[derive(Clone, Eq, PartialEq, prost::Message)]
pub struct Peer {
    #[prost(message, required, tag = 1)]
    pub id: PeerId,

    #[prost(string, required, tag = 2)]
    pub endpoint: String,

    #[prost(bool, required, tag = 3)]
    pub is_up: bool,

    #[prost(bool, required, tag = 4)]
    pub is_responding: bool,
}

impl From<OriginatorInfo> for Peer {
    fn from(value: OriginatorInfo) -> Self {
        Self {
            id: value.sender_id,
            endpoint: value.sender_endpoint,
            is_up: true,
            is_responding: true,
        }
    }
}

impl Peer {
    /// Create a new instance of [`Peer`]
    /// When creating a new peer, the [`Peer`] will be considered up and running by default
    pub fn new(id: impl Into<PeerId>, endpoint: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            is_up: true,
            is_responding: true,
        }
    }

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

    #[cfg(test)]
    pub(crate) fn set_down(mut self) -> Self {
        self.is_up = false;
        self
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}]", self.id.value(), self.endpoint)?;
        Ok(())
    }
}
