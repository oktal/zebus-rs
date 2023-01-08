use super::IntoResponse;

/// Zebus handler of a `T` typed message.
/// Zebus handlers must implemented this trait to be able to handle particular
/// messages
pub trait Handler<T> {
    type Response: IntoResponse;

    /// Handle `message`
    fn handle(&mut self, message: T) -> Self::Response;
}
