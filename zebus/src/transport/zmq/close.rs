use std::{
    io::Write,
    task::Poll,
    time::{Duration, Instant},
};

use futures_core::{Future, Stream};
use pin_project::pin_project;
use prost::Message;

use crate::{core::MessagePayload, transport::TransportMessage, Command, Peer};

use super::{outbound::ZmqOutboundSocket, ZmqError};

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Transport")]
pub(super) struct EndOfStream {}

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Abc.Zebus.Transport")]
pub(super) struct EndOfStreamAck {}

#[pin_project]
struct CloseFuture {
    #[pin]
    rx: crate::sync::stream::BroadcastStream<TransportMessage>,

    start: Instant,
}

impl Future for CloseFuture {
    type Output = Duration;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();

        match this.rx.poll_next(cx) {
            Poll::Ready(Some(msg)) => {
                if msg.is::<EndOfStreamAck>() {
                    Poll::Ready(this.start.elapsed())
                } else {
                    Poll::Pending
                }
            }
            _ => Poll::Pending,
        }
    }
}

pub(super) fn close(
    sender: &Peer,
    environment: String,
    socket: &mut ZmqOutboundSocket,
    rx: tokio::sync::broadcast::Receiver<TransportMessage>,
) -> super::Result<impl Future<Output = Duration>> {
    let start = Instant::now();
    let (_, end_of_stream) = TransportMessage::create(sender, environment, &EndOfStream {});
    let buf = end_of_stream.encode_to_vec();

    let rx = crate::sync::stream::BroadcastStream::from(rx);

    socket.write_all(&buf).map_err(ZmqError::Io)?;
    Ok(CloseFuture { rx, start })
}
