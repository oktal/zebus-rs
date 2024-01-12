use crate::dispatch::DispatchRequest;

use super::{Extract, FromRef};

/// An extractor to extract the associated state of the handler
#[derive(Debug, Copy, Clone)]
pub struct State<S>(pub S);

impl<InputRef, Output> Extract<InputRef> for State<Output>
where
    Output: FromRef<InputRef>,
{
    type Rejection = ();

    fn extract(_req: &DispatchRequest, state: &InputRef) -> Result<Self, Self::Rejection> {
        Ok(Self(Output::from_ref(state)))
    }
}
