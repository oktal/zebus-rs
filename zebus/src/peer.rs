use std::fmt;

use crate::PeerId;

#[derive(Clone, prost::Message)]
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

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer({}, {})", self.id.value(), self.endpoint)?;
        Ok(())
    }
}
