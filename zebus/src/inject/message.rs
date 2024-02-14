use thiserror::Error;

use crate::{
    core::{response::InternalError, MessageDescriptor, MessagePayload},
    dispatch::{DispatchMessage, DispatchRequest},
};

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("invalid message type. Got {got} expected {expected}")]
    InvalidMessageType { got: String, expected: &'static str },

    #[error("failed to decode message {0}: {1}")]
    Decode(&'static str, prost::DecodeError),
}

pub(crate) struct Message<M>(pub(crate) M)
where
    M: crate::Message + crate::MessageDescriptor + prost::Message + Clone + Default;

impl<S, M> super::Extract<S> for Message<M>
where
    M: crate::Message + crate::MessageDescriptor + prost::Message + Clone + Default,
{
    type Rejection = InternalError<Error>;

    fn extract(req: &DispatchRequest, _state: &S) -> Result<Self, Self::Rejection> {
        match req.message() {
            DispatchMessage::Remote(msg) => match msg.decode_as::<M>() {
                Some(msg) => msg
                    .map(Self)
                    .map_err(|e| InternalError(Error::Decode(<M as MessageDescriptor>::name(), e))),
                None => Err(InternalError(Error::InvalidMessageType {
                    got: req.message_type().to_string(),
                    expected: <M as MessageDescriptor>::name(),
                })),
            },
            DispatchMessage::Local(msg) => msg
                .downcast_ref::<M>()
                .ok_or_else(|| {
                    InternalError(Error::InvalidMessageType {
                        got: msg.name().to_string(),
                        expected: <M as MessageDescriptor>::name(),
                    })
                })
                .map(|msg| Self(msg.clone())),
        }
    }
}
