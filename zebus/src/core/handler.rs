use super::IntoResponse;
use crate::Bus;

pub struct Context<'a> {
    pub bus: &'a dyn Bus,
}

impl<'a> Context<'a> {
    pub(crate) fn new(bus: &'a dyn Bus) -> Self {
        Self { bus }
    }
}

/// A trait for Zebus message handlers
///
/// Types must implement this trait to be able to handle [`crate::Command`] commands
/// or [`crate::Event`] events
#[async_trait::async_trait]
pub trait Handler<T> {
    /// [`crate::Response`] returned to the originator of the message
    type Response: IntoResponse;

    /// Handle `message`
    async fn handle(&mut self, message: T, ctx: Context<'_>) -> Self::Response;
}

pub(crate) trait InternalHandler<T> {
    fn handle(&mut self, message: T);
}
