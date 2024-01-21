//! Utilities and combinators for [`Stream`]
#![allow(dead_code)]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use pin_project::pin_project;
use tokio::sync::broadcast::{self, error::SendError};

/// Extension trait for [`Stream`]
pub(crate) trait StreamExt: Stream {
    /// A stream that will consume exactly `n` items of the underlying `stream` and will yield
    /// items as [`Option<T>`]
    /// If the underlying stream has less items than requested, the stream will yield `None` for the item.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # futures::executor::block_on(async {
    /// use futures::stream;
    ///
    /// let stream = stream::iter(1..=3).take_exact(5);
    ///
    /// assert_eq!(stream.collect::<Vec<_>>().await, vec![Some(1), Some(2), Some(3), None, None])
    /// # });
    /// ```
    fn take_exact(self, n: usize) -> TakeExact<Self>
    where
        Self: Sized,
    {
        TakeExact::new(self, n)
    }
}

impl<St> StreamExt for St where St: Stream {}

/// A `BroadcastStream` wrapper similar to tokio's `BroadcastStream` wrapper
/// except that this version will directly yield items instead of `Result`
#[pin_project]
pub struct BroadcastStream<T> {
    /// Inner stream
    #[pin]
    inner: tokio_stream::wrappers::BroadcastStream<T>,
}

impl<T: 'static + Clone + Send> From<broadcast::Receiver<T>> for BroadcastStream<T> {
    fn from(value: broadcast::Receiver<T>) -> Self {
        Self {
            inner: value.into(),
        }
    }
}

impl<T: 'static + Clone + Send> Stream for BroadcastStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Err(_))) => Poll::Ready(None),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream for `take_exact()`
#[pin_project]
pub(crate) struct TakeExact<St> {
    /// The inner stream to consume
    #[pin]
    stream: St,

    /// Amount of items to consume from the stream
    n: usize,

    /// Current index
    cur: usize,
}

impl<St> TakeExact<St> {
    pub(crate) fn new(stream: St, n: usize) -> Self {
        Self { stream, n, cur: 0 }
    }
}

impl<St> Stream for TakeExact<St>
where
    St: Stream,
{
    type Item = Option<St::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        *this.cur += 1;

        // We hit the number of items we wanted to consume, stop the stream
        if this.cur > this.n {
            Poll::Ready(None)
        } else {
            match this.stream.as_mut().poll_next(cx) {
                // Stream yielded an item, yield `Some(item)`
                Poll::Ready(Some(item)) => Poll::Ready(Some(Some(item))),
                // Stream is not ready or has terminated, yield `None`
                _ => Poll::Ready(Some(None)),
            }
        }
    }
}

/// A stream of events
#[derive(Clone)]
pub struct EventStream<E> {
    tx: broadcast::Sender<E>,
}

/// A boxed stream of events
pub type BoxEventStream<E> = Pin<Box<dyn Stream<Item = E> + Send + Sync + 'static>>;

impl<E> EventStream<E>
where
    E: Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    pub(crate) fn send(&self, event: E) -> Result<usize, SendError<E>> {
        self.tx.send(event)
    }
}

impl<E> EventStream<E>
where
    E: Clone + Send + 'static,
{
    pub(crate) fn stream(&self) -> impl Stream<Item = E> {
        let rx = self.tx.subscribe();
        BroadcastStream::from(rx)
    }

    pub(crate) fn boxed(&self) -> BoxEventStream<E> {
        Box::pin(self.stream())
    }
}
