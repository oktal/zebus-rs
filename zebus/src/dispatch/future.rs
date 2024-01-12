use std::{sync::Arc, task::Poll};

use futures_core::Future;
use futures_util::{
    future::{Either, Ready},
    FutureExt,
};
use pin_project::pin_project;
use tokio::sync::mpsc;

use super::{DispatchError, DispatchRequest, Dispatched, HandlerResponse};
use crate::{core::Response, MessageTypeDescriptor};

struct Context {
    request: Arc<DispatchRequest>,
    descriptor: MessageTypeDescriptor,
    responses: Vec<Response>,
    errors: DispatchError,
}

impl Context {
    fn new(request: Arc<DispatchRequest>, descriptor: MessageTypeDescriptor) -> Self {
        Self {
            request,
            descriptor,
            responses: vec![],
            errors: Default::default(),
        }
    }

    fn set_response(&mut self, mut handler_response: HandlerResponse) {
        let response = handler_response.take_response();
        if let Some(response) = response {
            if let Response::StandardError(e) = response {
                self.errors.add(handler_response.handler_type(), e);
            } else {
                self.responses.push(response);
            }
        }
    }
}

impl Into<Dispatched> for Context {
    fn into(mut self) -> Dispatched {
        let request = self.request;
        let descriptor = self.descriptor;
        let result = if self.errors.is_empty() {
            Ok(self.responses.pop())
        } else {
            Err(self.errors)
        };

        Dispatched {
            request,
            descriptor,
            result,
        }
    }
}

/// A [`Future`] that is immediately ready with a [`super::Error`]
pub(crate) type Error = Ready<Result<Dispatched, super::Error>>;

/// A [`Future`] that will resolve when all the invokers for a [`DispatchRequest`]
/// have been called
#[pin_project]
pub(crate) struct Dispatching {
    /// Total number of invokers that should be called for the current request
    count: usize,

    /// Context
    context: Option<Context>,

    /// Reception channel for result of invokers execution
    #[pin]
    rx: mpsc::Receiver<HandlerResponse>,
}

/// Type alias for a dispatch future
/// This future will either be currently active with a [`Dispatching`] or will hold an immediately
/// ready [`Error`]
pub(crate) type DispatchFuture = Either<Dispatching, Error>;

impl Dispatching {
    fn new(
        count: usize,
        request: Arc<DispatchRequest>,
        descriptor: MessageTypeDescriptor,
        rx: mpsc::Receiver<HandlerResponse>,
    ) -> Self {
        Self {
            count,
            context: Some(Context::new(request, descriptor)),
            rx,
        }
    }

    fn into_dispatched(ctx: &mut Option<Context>) -> Dispatched {
        ctx.take().expect("missing context").into()
    }
}

impl Future for Dispatching {
    type Output = Result<Dispatched, super::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        // Poll the underlying `rx` channel
        match this.rx.poll_recv(cx) {
            // A response for a handler invocation has been received
            Poll::Ready(Some(response)) => {
                // Add the response to the current context
                this.context
                    .as_mut()
                    .expect("missing context")
                    .set_response(response);

                let count = this.count;
                *count -= 1;

                // If we received all the responses, we can resolve the future
                if *count == 0 {
                    Poll::Ready(Ok(Self::into_dispatched(this.context)))
                } else {
                    // Keep waiting for other handler invocation responses.
                    // We need to wake the task to make sure the future keeps getting polled
                    // waiting for new responses to come in
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            // The channel has been closed
            Poll::Ready(None) => {
                let count = *this.count;
                // If we received all the responses, we can resolve the future
                if count == 0 {
                    Poll::Ready(Ok(Self::into_dispatched(this.context)))
                } else {
                    // We won't be able to receive more responses since the channel has been
                    // closed but we are still waiting for some. Thus, we resolve the future
                    // with an error
                    Poll::Ready(Err(super::Error::Missing(count)))
                }
            }
            // No response has been received, keep polling
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(super) fn new(
    count: usize,
    request: Arc<DispatchRequest>,
    descriptor: MessageTypeDescriptor,
    rx: mpsc::Receiver<HandlerResponse>,
) -> DispatchFuture {
    Dispatching::new(count, request, descriptor, rx).left_future()
}

pub(super) fn err(error: super::Error) -> DispatchFuture {
    futures_util::future::err(error).right_future()
}
