use crate::DispatchHandler;
use std::sync::{Arc, Mutex};

use super::{
    commands::PingPeerCommand, PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted,
    PeerStopped,
};

struct Inner {}

pub(crate) struct Handler {
    inner: Arc<Mutex<Inner>>,
}

pub(crate) struct Client {
    inner: Arc<Mutex<Inner>>,
}

impl Client {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {})),
        }
    }

    pub(crate) fn handler(&self) -> Box<Handler> {
        Box::new(Handler {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl DispatchHandler for Handler {
    const DISPATCH_QUEUE: &'static str = crate::DEFAULT_DISPATCH_QUEUE;
}

impl crate::Handler<PeerStarted> for Handler {
    fn handle(&mut self, message: PeerStarted) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerStopped> for Handler {
    fn handle(&mut self, message: PeerStopped) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerDecommissioned> for Handler {
    fn handle(&mut self, message: PeerDecommissioned) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerNotResponding> for Handler {
    fn handle(&mut self, message: PeerNotResponding) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerResponding> for Handler {
    fn handle(&mut self, message: PeerResponding) {
        println!("{message:?}")
    }
}

impl crate::Handler<PingPeerCommand> for Handler {
    fn handle(&mut self, _message: PingPeerCommand) {
        println!("PING");
    }
}
