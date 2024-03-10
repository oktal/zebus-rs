use crate::{
    dispatch::{DispatchMessage, DispatchRequest},
    transport::OriginatorInfo,
};

use super::Extract;

/// An [`Extract`] extractor to inject [`OriginatorInfo`] originator of a
/// [`Message`](crate::Message)
pub struct Originator(pub OriginatorInfo);

impl<S> Extract<S> for Originator {
    type Rejection = ();

    fn extract(req: &DispatchRequest, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(match req.message() {
            DispatchMessage::Remote(message) => Self(message.originator.clone()),
            // TODO(oktal): properly fill `sender_machine_name` and `initiator_user_name` fields
            DispatchMessage::Local { self_peer, .. } => Self(OriginatorInfo {
                sender_id: self_peer.id.clone(),
                sender_endpoint: self_peer.endpoint.clone(),
                sender_machine_name: None,
                initiator_user_name: None,
            }),
        })
    }
}
