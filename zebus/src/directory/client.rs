use thiserror::Error;

use crate::{
    routing::tree::PeerSubscriptionTree, transport::TransportMessage, Handler, MessageTypeId,
    NoError, PeerId,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    commands::PingPeerCommand, PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted,
    PeerStopped,
};

/// Entry representing a peer from the directory
struct PeerEntry {}

struct Inner {
    /// Tree of subscriptions per [`MessageTypeId`]
    subscriptions: HashMap<MessageTypeId, PeerSubscriptionTree>,
}

impl Inner {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    fn handle(&mut self, peer_started: PeerStarted) {}
}

#[derive(Handler)]
pub(crate) struct DirectoryHandler {
    inner: Arc<Mutex<Inner>>,
}

pub(crate) struct Client {
    inner: Arc<Mutex<Inner>>,
}

impl Client {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    pub(crate) fn replay(&mut self, mut messages: Vec<TransportMessage>) -> Vec<TransportMessage> {
        messages.retain(|m| self.replay_message(m));
        messages
    }

    pub(crate) fn handler(&self) -> Box<DirectoryHandler> {
        Box::new(DirectoryHandler {
            inner: Arc::clone(&self.inner),
        })
    }

    fn replay_message(&mut self, message: &TransportMessage) -> bool {
        macro_rules! try_replay {
            ($ty: ty) => {
                if let Some(Ok(event)) = message.decode_as::<$ty>() {
                    self.inner.lock().unwrap().handle(event);
                    return true;
                }
            };
        }

        try_replay!(PeerStarted);
        false
    }
}

impl crate::Handler<PeerStarted> for DirectoryHandler {
    fn handle(&mut self, message: PeerStarted) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerStopped> for DirectoryHandler {
    fn handle(&mut self, message: PeerStopped) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerDecommissioned> for DirectoryHandler {
    fn handle(&mut self, message: PeerDecommissioned) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerNotResponding> for DirectoryHandler {
    fn handle(&mut self, message: PeerNotResponding) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerResponding> for DirectoryHandler {
    fn handle(&mut self, message: PeerResponding) {
        println!("{message:?}")
    }
}

impl crate::Handler<PingPeerCommand> for DirectoryHandler {
    fn handle(&mut self, _message: PingPeerCommand) {
        println!("PING");
    }
}
