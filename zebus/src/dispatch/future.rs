use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use super::DispatchResult;

struct Repr {
    /// Flag to indicate whether the dispatch is completed
    completed: bool,

    /// Optional result
    result: Option<DispatchResult>,

    /// Waker to use to wake the `tokio` runtime and poll the underlying future
    waker: Option<Waker>,
}

impl Repr {
    fn new() -> Self {
        Self {
            completed: false,
            result: None,
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
    pub(super) fn new() -> Self {
        Self {
            repr: Arc::new(Mutex::new(Repr::new())),
        }
    }

    pub(super) fn set_handled(&self, result: DispatchResult) {
        let mut state = self.repr.lock().unwrap();
        state.result = Some(result);
    }

    pub(super) fn set_completed(&self) {
        let mut state = self.repr.lock().unwrap();
        state.completed = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    pub(super) fn has_error(&self) -> bool {
        let state = self.repr.lock().unwrap();
        state.result.as_ref().map(|r| r.is_error()).unwrap_or(false)
    }
}

impl Future for DispatchFuture {
    type Output = Option<DispatchResult>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state = self.repr.lock().unwrap();
        if state.completed {
            Poll::Ready(state.result.take())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
