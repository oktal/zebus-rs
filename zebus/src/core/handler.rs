use crate::Bus;

use super::IntoResponse;

pub struct Context<'a> {
    pub bus: &'a dyn Bus,
}

/// A trait for Zebus message handlers
///
/// Types must implement this trait to be able to handle `[crate::Command`] commands
/// or [`crate::Event`] events
pub trait Handler<T> {
    /// [`crate::Response`] returned to the originator of the message
    type Response: IntoResponse;

    /// Handle `message`
    fn handle(&mut self, message: T) -> Self::Response;
}

pub trait ContextAwareHandler<T> {
    type Response: IntoResponse;

    fn handle(&mut self, message: T, ctx: Context<'_>) -> Self::Response;
}
