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
            // TODO(oktal): add implementation for local dispatch
            DispatchMessage::Local(_) => unimplemented!(),
        })
    }
}
