use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use super::{DispatchError, DispatchRequest, Dispatched};
use crate::{core::Response, transport::TransportMessage, MessageKind};

struct Context {
    request: DispatchRequest,
    kind: Option<MessageKind>,
    errors: DispatchError,
    response: Option<Response>,
}

impl Context {
    fn new(request: DispatchRequest) -> Self {
        Self {
            request,
            kind: None,
            errors: DispatchError::default(),
            response: None,
        }
    }

    fn set_response(&mut self, handler_type: &'static str, response: Option<Response>) {
        if let Some(response) = response {
            if let Response::StandardError(e) = response {
                self.errors.add(handler_type, e);
            } else {
                self.response = Some(response);
            }
        }
    }
}

impl Into<Dispatched> for Context {
    fn into(self) -> Dispatched {
        let request = self.request;
        let kind = self.kind.expect("missing message kind in dispatch context");
        let result = if self.errors.is_empty() {
            Ok(self.response)
        } else {
            Err(self.errors)
        };

        Dispatched {
            request,
            kind,
            result,
        }
    }
}

struct Repr {
    /// Flag to indicate whether the dispatch is completed
    completed: bool,

    /// Current dispatch context
    context: Option<Context>,

    /// Waker to use to wake the `tokio` runtime and poll the underlying future
    waker: Option<Waker>,
}

impl Repr {
    fn new(request: DispatchRequest) -> Self {
        Self {
            completed: false,
            context: Some(Context::new(request)),
            waker: None,
        }
    }
}

pub(crate) struct DispatchFuture {
    repr: Arc<Mutex<Repr>>,
}

impl Clone for DispatchFuture {
    fn clone(&self) -> Self {
        Self {
            repr: Arc::clone(&self.repr),
        }
    }
}

impl DispatchFuture {
    pub(super) fn new(request: DispatchRequest) -> Self {
        Self {
            repr: Arc::new(Mutex::new(Repr::new(request))),
        }
    }

    pub(super) fn set_kind(&self, kind: MessageKind) {
        self.apply_context(|ctx| ctx.kind = Some(kind));
    }

    pub(super) fn set_response(&self, handler_type: &'static str, response: Option<Response>) {
        self.apply_context(|ctx| ctx.set_response(handler_type, response));
    }

    pub(super) fn set_completed(self) {
        let mut state = self.repr.lock().unwrap();
        state.completed = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    fn apply_context(&self, f: impl FnOnce(&mut Context)) {
        let mut state = self.repr.lock().unwrap();
        if let Some(ref mut ctx) = state.context {
            f(ctx);
        }
    }
}

impl Future for DispatchFuture {
    type Output = Dispatched;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state = self.repr.lock().unwrap();
        if state.completed {
            Poll::Ready(state.context.take().expect("missing context").into())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
