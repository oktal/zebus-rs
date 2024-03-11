use super::PersistenceError;
use futures_core::{ready, Future};
use std::task::Poll;

#[pin_project::pin_project]
pub struct StopFuture {
    #[pin]
    handle: tokio::task::JoinHandle<Result<(), PersistenceError>>,
}

impl StopFuture {
    pub(super) fn spawn<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<(), PersistenceError>> + Send + 'static,
    {
        let handle = tokio::spawn(fut);
        Self { handle }
    }
}

impl Future for StopFuture {
    type Output = Result<(), PersistenceError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.handle.poll(cx));

        Poll::Ready(match res {
            Ok(res) => res,
            Err(e) => Err(PersistenceError::Join(e)),
        })
    }
}
