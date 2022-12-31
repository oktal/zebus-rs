use itertools::Itertools;
use tokio::sync::mpsc;

use crate::{
    proto::{self, PeerDescriptor},
    routing::tree::PeerSubscriptionTree,
    transport::TransportMessage,
    BindingKey, Handler, Peer, PeerId,
};
use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::{Arc, Mutex},
};

use super::{
    commands::{PingPeerCommand, RegisterPeerResponse},
    event::PeerEvent,
    PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
};

#[derive(Debug)]
struct SubscriptionIndex(HashMap<String, PeerSubscriptionTree>);

impl SubscriptionIndex {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add<'a>(
        &mut self,
        message_type: String,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        let tree = self
            .0
            .entry(message_type)
            .or_insert(PeerSubscriptionTree::new());
        for key in bindings {
            tree.add(peer.clone(), key);
        }
    }

    fn remove<'a>(
        &mut self,
        message_type: &String,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        if let Some(tree) = self.0.get_mut(message_type) {
            for key in bindings {
                tree.remove(peer, key);
            }
        }
    }
}

#[derive(Debug)]
struct SubscriptionEntry {
    binding_keys: HashSet<BindingKey>,
    timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
}

/// Entry representing a peer from the directory
#[derive(Debug, Default)]
struct PeerEntry {
    peer: Peer,
    is_persistent: bool,
    timestamp_utc: chrono::DateTime<chrono::Utc>,
    has_debugger_attached: bool,
    subscriptions: HashMap<String, SubscriptionEntry>,
}

impl PeerEntry {
    fn new(descriptor: PeerDescriptor) -> Self {
        let timestamp_utc = descriptor
            .timestamp_utc
            .and_then(|v| v.try_into().ok())
            .unwrap_or(chrono::Utc::now());

        Self {
            peer: descriptor.peer,
            is_persistent: descriptor.is_persistent,
            timestamp_utc,
            has_debugger_attached: descriptor.has_debugger_attached.unwrap_or(false),
            subscriptions: HashMap::new(),
        }
    }

    fn update(&mut self, descriptor: &PeerDescriptor) {
        let timestamp_utc = descriptor
            .timestamp_utc
            .and_then(|v| v.try_into().ok())
            .unwrap_or(chrono::Utc::now());

        self.peer.endpoint = descriptor.peer.endpoint.clone();
        self.peer.is_up = descriptor.peer.is_up;
        self.peer.is_responding = descriptor.peer.is_responding;
        self.timestamp_utc = timestamp_utc;
        self.has_debugger_attached = descriptor.has_debugger_attached.unwrap_or(false);
    }

    fn set_subscriptions(
        &mut self,
        subscriptions: Vec<proto::Subscription>,
        index: &mut SubscriptionIndex,
        timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let subscription_bindings = subscriptions
            .into_iter()
            .map(|s| (s.message_type_id, s.binding_key))
            .into_group_map();

        for (message_type, bindings) in subscription_bindings {
            let message_name = message_type.full_name;
            let binding_keys = bindings.into_iter().map(Into::into).collect::<HashSet<_>>();

            match self.subscriptions.entry(message_name.clone()) {
                hash_map::Entry::Occupied(mut e) => {
                    let entry = e.get_mut();

                    let to_remove = entry.binding_keys.difference(&binding_keys);
                    let to_add = binding_keys.difference(&entry.binding_keys);

                    index.add(message_name.clone(), &self.peer, to_add);
                    index.remove(&message_name, &self.peer, to_remove);

                    entry.timestamp_utc = timestamp_utc;
                    entry.binding_keys.retain(|b| binding_keys.contains(&b));
                }
                hash_map::Entry::Vacant(e) => {
                    index.add(message_name.clone(), &self.peer, binding_keys.iter());
                    e.insert(SubscriptionEntry {
                        binding_keys,
                        timestamp_utc,
                    });
                }
            }
        }
    }
}

struct PeerUpdate<'a> {
    id: &'a PeerId,
    events_tx: &'a mpsc::Sender<PeerEvent>,
}

impl PeerUpdate<'_> {
    fn raise(&self, event_fn: impl FnOnce(PeerId) -> PeerEvent) {
        if let Err(_) = self.events_tx.blocking_send(event_fn(self.id.clone())) {}
    }

    fn forget(self) {}
}

#[derive(Debug)]
struct Inner {
    subscriptions: SubscriptionIndex,
    peers: HashMap<PeerId, PeerEntry>,
    events_tx: mpsc::Sender<PeerEvent>,
}

impl Inner {
    fn new() -> (Self, tokio::sync::mpsc::Receiver<PeerEvent>) {
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(128);

        (
            Self {
                subscriptions: SubscriptionIndex::new(),
                peers: HashMap::new(),
                events_tx,
            },
            events_rx,
        )
    }

    fn add_or_update(&mut self, descriptor: PeerDescriptor) -> PeerUpdate {
        let peer_id = descriptor.peer.id.clone();
        let subscriptions = descriptor.subscriptions.clone();
        let timestamp_utc = descriptor.timestamp_utc.clone();

        let peer_entry = self
            .peers
            .entry(peer_id.clone())
            .and_modify(|e| e.update(&descriptor))
            .or_insert_with(|| PeerEntry::new(descriptor));

        let timestamp_utc = timestamp_utc.and_then(|t| t.try_into().ok());
        peer_entry.set_subscriptions(subscriptions, &mut self.subscriptions, timestamp_utc);

        PeerUpdate {
            id: &peer_entry.peer.id,
            events_tx: &self.events_tx,
        }
    }
}

impl Handler<PeerStarted> for Inner {
    fn handle(&mut self, message: PeerStarted) {
        self.add_or_update(message.descriptor)
            .raise(PeerEvent::Started);
    }
}

#[derive(Handler)]
pub(crate) struct DirectoryHandler {
    inner: Arc<Mutex<Inner>>,
}

pub(crate) struct Client {
    inner: Arc<Mutex<Inner>>,
}

impl Client {
    pub(crate) fn start() -> (Self, tokio::sync::mpsc::Receiver<PeerEvent>) {
        let (inner, events_rx) = Inner::new();

        (
            Self {
                inner: Arc::new(Mutex::new(inner)),
            },
            events_rx,
        )
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

impl crate::Handler<RegisterPeerResponse> for Client {
    fn handle(&mut self, message: RegisterPeerResponse) {
        let mut inner = self.inner.lock().unwrap();
        for descriptor in message.peers {
            inner.add_or_update(descriptor).forget();
        }
    }
}

impl crate::Handler<PeerStarted> for DirectoryHandler {
    fn handle(&mut self, message: PeerStarted) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
        println!("{inner:?}");
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
