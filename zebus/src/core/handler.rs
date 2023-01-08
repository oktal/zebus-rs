use super::IntoResponse;

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
