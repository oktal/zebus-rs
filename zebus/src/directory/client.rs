//! A client to communicate with a a peer directory
use itertools::Itertools;
use tokio::sync::mpsc;

use crate::{
    proto::{self, PeerDescriptor},
    routing::tree::PeerSubscriptionTree,
    transport::TransportMessage,
    BindingKey, Handler, MessageType, Peer, PeerId,
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
struct SubscriptionIndex(HashMap<MessageType, PeerSubscriptionTree>);

impl SubscriptionIndex {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add<'a>(
        &mut self,
        message_type: impl Into<MessageType>,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        let tree = self
            .0
            .entry(message_type.into())
            .or_insert(PeerSubscriptionTree::new());
        for key in bindings {
            tree.add(peer.clone(), key);
        }
    }

    fn remove<'a>(
        &mut self,
        message_type: &MessageType,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        if let Some(tree) = self.0.get_mut(&message_type) {
            for key in bindings {
                tree.remove(peer, key);
            }
        }
    }
}

/// Entry representing a subscription for a given message
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
    subscriptions: HashMap<MessageType, SubscriptionEntry>,
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

    fn checked(&self, timestamp: chrono::DateTime<chrono::Utc>) -> Option<&Self> {
        (timestamp >= self.timestamp_utc).then_some(self)
    }

    fn checked_mut(&mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Option<&mut Self> {
        (timestamp >= self.timestamp_utc).then_some(self)
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
            let message_type = MessageType::from(message_type.full_name);
            let binding_keys = bindings.into_iter().map(Into::into).collect::<HashSet<_>>();

            match self.subscriptions.entry(message_type.clone()) {
                hash_map::Entry::Occupied(mut e) => {
                    let entry = e.get_mut();

                    let to_remove = entry.binding_keys.difference(&binding_keys);
                    let to_add = binding_keys.difference(&entry.binding_keys);

                    index.add(message_type.clone(), &self.peer, to_add);
                    index.remove(&message_type, &self.peer, to_remove);

                    entry.timestamp_utc = timestamp_utc;
                    entry.binding_keys.retain(|b| binding_keys.contains(&b));
                }
                hash_map::Entry::Vacant(e) => {
                    index.add(message_type.clone(), &self.peer, binding_keys.iter());
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
    events_tx: mpsc::Sender<PeerEvent>,
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
            events_tx: self.events_tx.clone(),
        }
    }

    fn update_with<'a>(
        &mut self,
        peer_id: &'a PeerId,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
        f: impl FnOnce(&mut PeerEntry),
    ) -> Option<PeerUpdate<'a>> {
        let entry = self.entry_mut(peer_id, timestamp)?;
        f(entry);
        Some(PeerUpdate {
            id: peer_id,
            events_tx: self.events_tx.clone(),
        })
    }

    fn remove<'a>(&mut self, peer_id: &'a PeerId) -> Option<PeerUpdate<'a>> {
        self.peers.remove(peer_id)?;
        Some(PeerUpdate {
            id: peer_id,
            events_tx: self.events_tx.clone(),
        })
    }

    fn entry<'a>(
        &'a self,
        peer_id: &PeerId,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Option<&'a PeerEntry> {
        let entry = self.peers.get(peer_id)?;
        if let Some(timestamp) = timestamp {
            entry.checked(timestamp)
        } else {
            Some(entry)
        }
    }

    fn entry_mut<'a>(
        &'a mut self,
        peer_id: &PeerId,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Option<&'a mut PeerEntry> {
        let entry = self.peers.get_mut(peer_id)?;
        if let Some(timestamp) = timestamp {
            entry.checked_mut(timestamp)
        } else {
            Some(entry)
        }
    }
}

impl Handler<PeerStarted> for Inner {
    fn handle(&mut self, message: PeerStarted) {
        self.add_or_update(message.descriptor)
            .raise(PeerEvent::Started);
    }
}

impl Handler<PeerStopped> for Inner {
    fn handle(&mut self, message: PeerStopped) {
        let timestamp_utc = chrono::Utc::now();

        let update = self.update_with(&message.id, Some(timestamp_utc), |e| {
            e.timestamp_utc = timestamp_utc;
            e.peer.is_up = false;
            e.peer.is_responding = false;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Stopped);
        }
    }
}

impl Handler<PeerDecommissioned> for Inner {
    fn handle(&mut self, message: PeerDecommissioned) {
        let update = self.remove(&message.id);
        if let Some(update) = update {
            update.raise(PeerEvent::Decomissionned);
        }
    }
}

impl Handler<PeerNotResponding> for Inner {
    fn handle(&mut self, message: PeerNotResponding) {
        let update = self.update_with(&message.id, None, |e| {
            e.peer.is_responding = false;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
    }
}

impl Handler<PeerResponding> for Inner {
    fn handle(&mut self, message: PeerResponding) {
        let update = self.update_with(&message.id, None, |e| {
            e.peer.is_responding = true;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
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

    pub(crate) fn get(&self, peer_id: &PeerId) -> Option<Peer> {
        let inner = self.inner.lock().unwrap();
        inner.entry(peer_id, None).map(|e| e.peer.clone())
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
        try_replay!(PeerStopped);
        try_replay!(PeerNotResponding);
        try_replay!(PeerResponding);
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
    }
}

impl crate::Handler<PeerStopped> for DirectoryHandler {
    fn handle(&mut self, message: PeerStopped) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PeerDecommissioned> for DirectoryHandler {
    fn handle(&mut self, message: PeerDecommissioned) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerNotResponding> for DirectoryHandler {
    fn handle(&mut self, message: PeerNotResponding) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PeerResponding> for DirectoryHandler {
    fn handle(&mut self, message: PeerResponding) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PingPeerCommand> for DirectoryHandler {
    fn handle(&mut self, _message: PingPeerCommand) {
        println!("PING");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::IntoProtobuf;

    struct Fixture {
        client: Client,
        handler: Box<DirectoryHandler>,
        descriptor: PeerDescriptor,
        events_rx: mpsc::Receiver<PeerEvent>,
    }

    impl Fixture {
        fn new() -> Self {
            let (client, events_rx) = Client::start();
            let handler = client.handler();
            let descriptor = Self::create_descriptor();

            Self {
                client,
                handler,
                descriptor,
                events_rx,
            }
        }

        fn recv(&mut self) -> Option<PeerEvent> {
            self.events_rx.blocking_recv()
        }

        fn peer_id(&self) -> PeerId {
            self.descriptor.peer.id.clone()
        }

        fn peer(&self) -> Peer {
            self.descriptor.peer.clone()
        }

        fn create_descriptor() -> PeerDescriptor {
            let timestamp_utc = chrono::Utc::now();
            PeerDescriptor {
                peer: Peer::test(),
                subscriptions: vec![],
                is_persistent: true,
                timestamp_utc: Some(timestamp_utc.into_protobuf()),
                has_debugger_attached: Some(false),
            }
        }

        fn create_descriptors(n: usize) -> Vec<PeerDescriptor> {
            Self::create_descriptors_with(n, |_| Self::create_descriptor())
        }

        fn create_descriptors_with(n: usize, create_fn: impl Fn(usize) -> PeerDescriptor) -> Vec<PeerDescriptor> {
            (0..n)
                .into_iter()
                .map(create_fn)
                .collect()
        }
    }

    #[test]
    fn handle_registration() {
        let mut fixture = Fixture::new();
        let descriptors = Fixture::create_descriptors(5);
        fixture.client.handle(RegisterPeerResponse {
            peers: descriptors.clone(),
        });

        for descriptor in &descriptors {
            let peer = &descriptor.peer;

            assert_eq!(fixture.client.get(&peer.id), Some(peer.clone()));
        }
        assert_eq!(fixture.client.get(&PeerId::new("Test.Peer.0")), None);
    }

    #[test]
    fn handle_peer_started() {
        let mut fixture = Fixture::new();
        fixture.handler.handle(PeerStarted {
            descriptor: fixture.descriptor.clone(),
        });

        assert_eq!(fixture.client.get(&fixture.peer_id()), Some(fixture.peer()));
        assert_eq!(fixture.client.get(&PeerId::new("Test.Peer.0")), None);
        assert_eq!(fixture.recv(), Some(PeerEvent::Started(fixture.peer_id())));
    }

    #[test]
    fn handle_peer_stopped() {
        let mut fixture = Fixture::new();
        let peer = fixture.peer();
        let timestamp_utc = chrono::Utc::now();

        fixture.handler.handle(PeerStarted {
            descriptor: fixture.descriptor.clone(),
        });
        fixture.handler.handle(PeerStopped {
            id: peer.id.clone(),
            endpoint: Some(peer.endpoint.clone()),
            timestamp_utc: Some(timestamp_utc.into_protobuf()),
        });

        let peer_stopped = fixture.client.get(&fixture.peer_id());
        assert_eq!(
            peer_stopped,
            Some(Peer {
                id: peer.id.clone(),
                endpoint: peer.endpoint.clone(),
                is_up: false,
                is_responding: false
            })
        );

        assert_eq!(fixture.recv(), Some(PeerEvent::Started(fixture.peer_id())));
        assert_eq!(fixture.recv(), Some(PeerEvent::Stopped(fixture.peer_id())));
    }
}
