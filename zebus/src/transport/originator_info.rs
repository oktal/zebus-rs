use crate::{proto::prost, PeerId};

#[derive(Clone, prost::Message)]
pub struct OriginatorInfo {
    #[prost(message, required, tag = "1")]
    pub sender_id: PeerId,

    #[prost(string, required, tag = "2")]
    pub sender_endpoint: String,

    #[prost(string, optional, tag = "3")]
    pub sender_machine_name: Option<String>,

    #[prost(string, optional, tag = "4")]
    pub initiator_user_name: Option<String>,
}
