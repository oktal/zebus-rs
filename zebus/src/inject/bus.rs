use std::sync::Arc;

use super::Extract;

/// An extractor to extract a [`Bus`]
pub struct Bus(pub Arc<dyn crate::Bus>);

impl<S> Extract<S> for Bus {
    type Rejection = ();

    fn extract(
        req: &crate::dispatch::DispatchRequest,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(req.bus()))
    }
}
