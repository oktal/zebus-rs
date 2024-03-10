use std::{marker::PhantomData, task::Poll};

use futures_core::Future;

use crate::Peer;

use super::{SendContext, TransportMessage};

pub(crate) struct SendEntry {
    pub(crate) peers: Vec<Peer>,
    pub(crate) message: TransportMessage,
    pub(crate) context: SendContext,
}

pub(crate) struct SendError(());

#[pin_project::pin_project]
pub struct SendFuture<T, E> {
    #[pin]
    tx: tokio_util::sync::PollSender<T>,
    entry: Option<SendEntry>,
    _phantom: PhantomData<E>,
}

impl<T, E> SendFuture<T, E>
where
    T: Send + 'static,
{
    pub(crate) fn new(
        tx: tokio::sync::mpsc::Sender<T>,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        context: SendContext,
    ) -> Self {
        Self {
            tx: tokio_util::sync::PollSender::new(tx),
            entry: Some(SendEntry {
                peers: peers.collect(),
                message,
                context,
            }),
            _phantom: PhantomData,
        }
    }
}

impl<T, E> Future for SendFuture<T, E>
where
    T: From<SendEntry> + Send,
    E: From<SendError>,
{
    type Output = Result<(), E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        match this.tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(
                match this
                    .tx
                    .send_item(this.entry.take().expect("invalid state").into())
                {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        let _inner = e.into_inner().expect("send_item should return entry");
                        Err(E::from(SendError(())))
                    }
                },
            ),
            Poll::Ready(Err(_)) => Poll::Ready(Err(E::from(SendError(())))),
            Poll::Pending => Poll::Pending,
        }
    }
}
