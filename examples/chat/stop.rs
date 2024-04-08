use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Poll, Waker},
};

struct State {
    signaled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

pub struct StopSignal {
    state: Arc<State>,
}

impl StopSignal {
    pub fn new() -> anyhow::Result<Self> {
        let state = Arc::new(State {
            signaled: AtomicBool::new(false),
            waker: Mutex::new(None),
        });

        let handler_state = state.clone();
        ctrlc::set_handler(move || {
            handler_state.signaled.store(true, Ordering::SeqCst);

            let mut waker = handler_state.waker.lock().expect("lock poisoned");
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        })?;

        Ok(Self { state })
    }
}

impl Future for StopSignal {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.state.signaled.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }

        *self.state.waker.lock().expect("lock poisoned") = Some(cx.waker().clone());
        Poll::Pending
    }
}
