use std::{task::Poll, time::Duration};

use futures_core::{ready, Future, Stream};
use futures_util::StreamExt;

use crate::BusConfiguration;

use super::{PersistenceError, PersistenceEvent};

async fn timeout<S>(
    mut stream: S,
    duration: Duration,
    tx: tokio::sync::oneshot::Sender<Result<(), PersistenceError>>,
) where
    S: Stream<Item = PersistenceEvent> + Unpin,
{
    let res = match tokio::time::timeout(duration, stream.next()).await {
        Ok(_) => Ok(()),
        Err(_) => Err(PersistenceError::Unreachable(duration)),
    };

    let _ = tx.send(res);
}

#[pin_project::pin_project]
pub struct StartFuture {
    #[pin]
    rx: tokio::sync::oneshot::Receiver<Result<(), PersistenceError>>,
}

#[pin_project::pin_project]
pub struct StopFuture {
    #[pin]
    handle: tokio::task::JoinHandle<Result<(), PersistenceError>>,
}

impl StartFuture {
    pub(super) fn spawn<S>(stream: S, configuration: &BusConfiguration) -> Self
    where
        S: Stream<Item = PersistenceEvent> + Send + Unpin + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(timeout(stream, configuration.start_replay_timeout, tx));
        Self { rx }
    }
}

impl Future for StartFuture {
    type Output = Result<(), PersistenceError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.rx.poll(cx));

        Poll::Ready(match res {
            Ok(res) => res,
            Err(_) => Err(PersistenceError::InvalidOperation),
        })
    }
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
