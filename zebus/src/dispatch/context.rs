use std::sync::Arc;

use tokio::sync::mpsc;

use crate::Context;

use super::{DispatchMessage, DispatchRequest, HandlerResponse};

#[derive(Clone)]
pub(crate) struct DispatchContext {
    request: Arc<DispatchRequest>,
    tx: mpsc::Sender<HandlerResponse>,
}

impl DispatchContext {
    fn new(request: Arc<DispatchRequest>) -> (Self, mpsc::Receiver<HandlerResponse>) {
        let (tx, rx) = mpsc::channel(16);
        (
            Self {
                request: Arc::clone(&request),
                tx,
            },
            rx,
        )
    }

    pub(super) fn request(&self) -> Arc<DispatchRequest> {
        Arc::clone(&self.request)
    }

    pub(crate) fn handler_context(&self) -> Context<'_> {
        Context::new(self.request.bus.as_ref())
    }

    pub(crate) fn message(&self) -> &DispatchMessage {
        &self.request.message
    }

    pub(crate) fn message_type(&self) -> &str {
        self.message().message_type()
    }

    pub(super) async fn send(&self, response: HandlerResponse) {
        let _ = self.tx.send(response).await;
    }
}

pub(super) fn new(request: DispatchRequest) -> (DispatchContext, mpsc::Receiver<HandlerResponse>) {
    DispatchContext::new(Arc::new(request))
}
