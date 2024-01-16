use crate::{dispatch::DispatchRequest, IntoResponse};

pub mod bus;
pub(crate) mod message;
pub use bus::Bus;
pub mod originator;
pub use originator::Originator;
pub mod state;
pub use state::State;

// TODO(oktal): add documentation
pub trait Extract<S>: Sized {
    type Rejection: IntoResponse;

    fn extract(req: &DispatchRequest, state: &S) -> Result<Self, Self::Rejection>;
}

// TODO(oktal): add documentation
pub trait FromRef<T> {
    fn from_ref(input: &T) -> Self;
}

impl<T> FromRef<T> for T
where
    T: Clone,
{
    fn from_ref(input: &T) -> Self {
        input.clone()
    }
}

impl<S> Extract<S> for () {
    type Rejection = ();

    fn extract(_req: &DispatchRequest, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(())
    }
}
