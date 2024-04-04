//! A client to communicate with a a peer directory
use chrono::Utc;
use itertools::Itertools;
use tokio::sync::broadcast;
use tracing::debug;

use crate::dispatch::handler::{InvokerHandler, MessageHandler};
use crate::message_type_id::MessageTypeId;
use crate::proto::FromProtobuf;
use crate::{
    core::MessagePayload, routing::tree::PeerSubscriptionTree, transport::TransportMessage,
    BindingKey, Peer, PeerId,
};
use crate::{inject, Subscription};
use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::{Arc, Mutex},
};

use super::{
    commands::{PingPeerCommand, RegisterPeerResponse},
    event::PeerEvent,
    events::{PeerSubscriptionsForTypesUpdated, SubscriptionsForType},
    Directory, DirectoryReader, PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted,
    PeerStopped,
};
use super::{MessageBinding, PeerDescriptor};

#[derive(Debug)]
struct SubscriptionIndex(HashMap<String, PeerSubscriptionTree>);

impl SubscriptionIndex {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add<'a>(
        &mut self,
        message_type: impl Into<MessageTypeId>,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        let message_type = message_type.into();

        let tree = self
            .0
            .entry(message_type.into_name())
            .or_insert(PeerSubscriptionTree::new());
        for key in bindings {
            tree.add(peer.clone(), key);
        }
    }

    fn get(&self, message_type: &MessageTypeId, binding: &BindingKey) -> Vec<Peer> {
        match self.0.get(message_type.full_name()) {
            Some(tree) => tree.get_peers(binding),
            None => vec![],
        }
    }

    fn remove<'a>(
        &mut self,
        message_type: &MessageTypeId,
        peer: &Peer,
        bindings: impl Iterator<Item = &'a BindingKey>,
    ) {
        if let Some(tree) = self.0.get_mut(message_type.full_name()) {
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
    timestamp_utc: chrono::DateTime<chrono::Utc>,
}

impl SubscriptionEntry {
    fn checked_mut(&mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Option<&mut Self> {
        (timestamp >= self.timestamp_utc).then_some(self)
    }
}

/// Entry representing a peer from the directory
#[derive(Debug, Default)]
struct PeerEntry {
    descriptor: PeerDescriptor,
    timestamp_utc: chrono::DateTime<chrono::Utc>,
    subscriptions: HashMap<MessageTypeId, SubscriptionEntry>,
}

impl PeerEntry {
    fn new(descriptor: PeerDescriptor) -> Self {
        let timestamp_utc = descriptor.timestamp_utc.unwrap_or(chrono::Utc::now());

        Self {
            descriptor,
            timestamp_utc,
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
        let timestamp_utc = descriptor.timestamp_utc.unwrap_or(chrono::Utc::now());

        self.descriptor.peer.endpoint = descriptor.peer.endpoint.clone();
        self.descriptor.peer.is_up = descriptor.peer.is_up;
        self.descriptor.peer.is_responding = descriptor.peer.is_responding;
        self.timestamp_utc = timestamp_utc;
    }

    fn set_subscriptions(
        &mut self,
        index: &mut SubscriptionIndex,
        subscriptions: Vec<Subscription>,
        timestamp_utc: chrono::DateTime<chrono::Utc>,
    ) {
        let subscription_bindings = subscriptions
            .into_iter()
            .map(|s| (s.message_type_id, s.binding_key))
            .into_group_map();

        for (message_type, bindings) in subscription_bindings {
            let binding_keys = bindings.into_iter().collect::<HashSet<_>>();
            self.set_message_subscriptions(index, message_type, binding_keys, timestamp_utc);
        }
    }

    fn set_subscriptions_for(
        &mut self,
        index: &mut SubscriptionIndex,
        subscriptions: Vec<SubscriptionsForType>,
        timestamp_utc: chrono::DateTime<chrono::Utc>,
    ) {
        for subscription in subscriptions {
            let message_type = MessageTypeId::from_protobuf(subscription.message_type);
            let binding_keys = subscription
                .bindings
                .into_iter()
                .map(BindingKey::from_protobuf)
                .collect::<HashSet<_>>();
            self.set_message_subscriptions(index, message_type, binding_keys, timestamp_utc);
        }
    }

    fn set_message_subscriptions(
        &mut self,
        index: &mut SubscriptionIndex,
        message_type: MessageTypeId,
        binding_keys: HashSet<BindingKey>,
        timestamp_utc: chrono::DateTime<chrono::Utc>,
    ) {
        match self.subscriptions.entry(message_type.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let entry = e.get_mut();

                if let Some(entry) = entry.checked_mut(timestamp_utc) {
                    let to_remove = entry.binding_keys.difference(&binding_keys);
                    let to_add = binding_keys.difference(&entry.binding_keys);

                    index.add(message_type.clone(), &self.descriptor.peer, to_add);
                    index.remove(&message_type, &self.descriptor.peer, to_remove);

                    entry.timestamp_utc = timestamp_utc;
                    entry.binding_keys.retain(|b| binding_keys.contains(b));
                }
            }
            hash_map::Entry::Vacant(e) => {
                index.add(
                    message_type.clone(),
                    &self.descriptor.peer,
                    binding_keys.iter(),
                );
                e.insert(SubscriptionEntry {
                    binding_keys,
                    timestamp_utc,
                });
            }
        }
    }
}

struct PeerUpdate {
    descriptor: PeerDescriptor,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl PeerUpdate {
    fn raise(self, event_fn: impl FnOnce(PeerDescriptor) -> PeerEvent) {
        let _ = self.events_tx.send(event_fn(self.descriptor));
    }

    fn forget(self) {}
}

#[derive(Debug)]
pub struct DirectoryState {
    subscriptions: SubscriptionIndex,
    peers: HashMap<PeerId, PeerEntry>,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl DirectoryState {
    fn new() -> Self {
        let (events_tx, _) = broadcast::channel(128);

        Self {
            subscriptions: SubscriptionIndex::new(),
            peers: HashMap::new(),
            events_tx,
        }
    }

    fn peer_started(&mut self, message: PeerStarted) {
        self.add_or_update(PeerDescriptor::from_protobuf(message.descriptor))
            .raise(PeerEvent::Started);
    }

    fn peer_stopped(&mut self, message: PeerStopped) {
        let timestamp_utc = chrono::Utc::now();

        let update = self.update_with(&message.id, Some(timestamp_utc), |e| {
            e.timestamp_utc = timestamp_utc;
            e.descriptor.peer.is_up = false;
            e.descriptor.peer.is_responding = false;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Stopped);
        }
    }

    fn peer_decommissioned(&mut self, message: PeerDecommissioned) {
        let update = self.remove(&message.id);
        if let Some(update) = update {
            update.raise(PeerEvent::Decomissionned);
        }
    }

    fn peer_not_responding(&mut self, message: PeerNotResponding) {
        let update = self.update_with(&message.id, None, |e| {
            e.descriptor.peer.is_responding = false;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
    }

    fn peer_responding(&mut self, message: PeerResponding) {
        let update = self.update_with(&message.id, None, |e| {
            e.descriptor.peer.is_responding = true;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
    }

    fn peer_subscriptions_for_type_updated(&mut self, message: PeerSubscriptionsForTypesUpdated) {
        let timestamp_utc = message
            .timestamp_utc
            .try_into()
            .ok()
            .unwrap_or(chrono::Utc::now());

        let update =
            self.set_subscriptions_for(&message.peer_id, message.subscriptions, timestamp_utc);

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events_tx.subscribe()
    }

    fn add_or_update(&mut self, descriptor: PeerDescriptor) -> PeerUpdate {
        let peer_id = descriptor.peer.id.clone();

        let peer_entry = self
            .peers
            .entry(peer_id)
            .and_modify(|e| e.update(&descriptor))
            .or_insert_with(|| PeerEntry::new(descriptor.clone()));

        let index = &mut self.subscriptions;
        let timestamp_utc = descriptor.timestamp_utc.unwrap_or(Utc::now());

        peer_entry.set_subscriptions(index, descriptor.subscriptions.clone(), timestamp_utc);

        PeerUpdate {
            descriptor,
            events_tx: self.events_tx.clone(),
        }
    }

    fn set_subscriptions_for(
        &mut self,
        peer_id: &PeerId,
        subscriptions: Vec<SubscriptionsForType>,
        timestamp_utc: chrono::DateTime<chrono::Utc>,
    ) -> Option<PeerUpdate> {
        if let Some(entry) = self.peers.get_mut(peer_id) {
            if let Some(entry) = entry.checked_mut(timestamp_utc) {
                entry.set_subscriptions_for(&mut self.subscriptions, subscriptions, timestamp_utc);

                Some(PeerUpdate {
                    descriptor: entry.descriptor.clone(),
                    events_tx: self.events_tx.clone(),
                })
            } else {
                entry.set_subscriptions_for(&mut self.subscriptions, subscriptions, timestamp_utc);
                Some(PeerUpdate {
                    descriptor: entry.descriptor.clone(),
                    events_tx: self.events_tx.clone(),
                })
            }
        } else {
            None
        }
    }

    fn get_peers_handling(&self, binding: &MessageBinding) -> Vec<Peer> {
        // TODO(oktal):  avoid allocating a MessageTypeId
        let message_type = MessageTypeId::from(*binding.descriptor());
        self.subscriptions.get(&message_type, binding.key())
    }

    fn update_with(
        &mut self,
        peer_id: &PeerId,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
        f: impl FnOnce(&mut PeerEntry),
    ) -> Option<PeerUpdate> {
        let entry = self.entry_mut(peer_id, timestamp)?;
        f(entry);
        Some(PeerUpdate {
            descriptor: entry.descriptor.clone(),
            events_tx: self.events_tx.clone(),
        })
    }

    fn remove(&mut self, peer_id: &PeerId) -> Option<PeerUpdate> {
        let entry = self.peers.remove(peer_id)?;
        Some(PeerUpdate {
            descriptor: entry.descriptor,
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

pub(crate) struct Client {
    state: Arc<Mutex<DirectoryState>>,
}

impl Client {
    pub(crate) fn replay(&mut self, mut messages: Vec<TransportMessage>) -> Vec<TransportMessage> {
        messages.retain(|m| self.replay_message(m));
        messages
    }

    fn replay_message(&mut self, message: &TransportMessage) -> bool {
        macro_rules! try_replay {
            ($ty: ty, $handler: ident) => {
                if let Some(Ok(event)) = message.decode_as::<$ty>() {
                    self.state.lock().unwrap().$handler(event);
                    return true;
                }
            };
        }

        try_replay!(PeerStarted, peer_started);
        try_replay!(PeerStopped, peer_stopped);
        try_replay!(PeerNotResponding, peer_not_responding);
        try_replay!(PeerResponding, peer_responding);
        try_replay!(
            PeerSubscriptionsForTypesUpdated,
            peer_subscriptions_for_type_updated
        );
        false
    }
}

impl DirectoryReader for Client {
    fn get_peer(&self, peer_id: &PeerId) -> Option<PeerDescriptor> {
        let inner = self.state.lock().unwrap();
        inner.entry(peer_id, None).map(|e| e.descriptor.clone())
    }

    fn get_peers_handling(&self, binding: &MessageBinding) -> Vec<Peer> {
        let inner = self.state.lock().unwrap();
        inner.get_peers_handling(binding)
    }
}

impl Directory for Client {
    type EventStream = crate::sync::stream::BroadcastStream<PeerEvent>;
    type Handler = MessageHandler<Arc<Mutex<DirectoryState>>>;

    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(DirectoryState::new())),
        })
    }

    fn subscribe(&self) -> Self::EventStream {
        let inner = self.state.lock().unwrap();
        inner.subscribe().into()
    }

    fn handle_registration(&self, response: RegisterPeerResponse) {
        let mut inner = self.state.lock().unwrap();
        for descriptor in response.peers {
            inner
                .add_or_update(PeerDescriptor::from_protobuf(descriptor))
                .forget();
        }
    }

    fn handler(&self) -> Self::Handler {
        MessageHandler::with_state(Arc::clone(&self.state))
            .handles(peer_started.into_handler())
            .handles(peer_stopped.into_handler())
            .handles(peer_decommissioned.into_handler())
            .handles(peer_not_responding.into_handler())
            .handles(peer_responding.into_handler())
            .handles(ping_peer.into_handler())
            .handles(peer_subscriptions_for_type_updated.into_handler())
    }

    fn reader(&self) -> Arc<dyn DirectoryReader> {
        Arc::new(Self {
            state: Arc::clone(&self.state),
        })
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

async fn peer_started(
    message: PeerStarted,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state.lock().expect("poisoned mutex").peer_started(message)
}

async fn peer_stopped(
    message: PeerStopped,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state.lock().expect("poisoned mutex").peer_stopped(message)
}

async fn peer_decommissioned(
    message: PeerDecommissioned,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state
        .lock()
        .expect("poisoned mutex")
        .peer_decommissioned(message)
}

async fn peer_not_responding(
    message: PeerNotResponding,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state
        .lock()
        .expect("poisoned mutex")
        .peer_not_responding(message)
}

async fn peer_responding(
    message: PeerResponding,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state
        .lock()
        .expect("poisoned mutex")
        .peer_responding(message)
}

async fn ping_peer(_message: PingPeerCommand, inject::Originator(originator): inject::Originator) {
    let sender_id = originator.sender_id;
    let sender_endpoint = originator.sender_endpoint;
    debug!("Received PING from {sender_id} [{sender_endpoint}]",)
}

async fn peer_subscriptions_for_type_updated(
    message: PeerSubscriptionsForTypesUpdated,
    inject::State(state): inject::State<Arc<Mutex<DirectoryState>>>,
) {
    state
        .lock()
        .expect("poisoned mutex")
        .peer_subscriptions_for_type_updated(message)
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use chrono::Duration;
    use futures_util::FutureExt;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::bus::NoopBus;
    use crate::directory::event::PeerEventKind;
    use crate::directory::DirectoryReaderExt;
    use crate::dispatch::InvokerService;
    use crate::proto::IntoProtobuf;
    use crate::{Message, MessageDescriptor, MessageTypeId, Response};

    #[derive(Clone, crate::Command, prost::Message)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct RoutableCommand {
        #[zebus(routing_position = 1)]
        #[prost(string, tag = 1)]
        name: String,

        #[zebus(routing_position = 2)]
        #[prost(uint64, tag = 2)]
        id: u64,
    }

    #[derive(Clone, crate::Event, prost::Message)]
    #[zebus(namespace = "Abc.Test")]
    struct TestEvent {
        #[prost(string, tag = 1)]
        _outcome: String,
    }

    #[derive(Clone, crate::Event, prost::Message)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct RoutableTestEvent {
        #[zebus(routing_position = 1)]
        #[prost(string, tag = 1)]
        outcome: String,
    }

    struct Fixture {
        bus: Arc<NoopBus>,
        client: Arc<Client>,
        service: Box<dyn InvokerService>,
        descriptor: PeerDescriptor,
        events_rx: crate::sync::stream::BroadcastStream<PeerEvent>,
    }

    impl Fixture {
        fn new() -> Self {
            let client = Client::new();
            let events_rx = client.subscribe();
            let handler = client.handler();
            let descriptor = Self::create_descriptor();

            Self {
                bus: Arc::new(NoopBus),
                client,
                service: Box::new(handler),
                descriptor,
                events_rx,
            }
        }

        async fn recv(&mut self) -> Option<PeerEvent> {
            self.events_rx.next().await
        }

        async fn try_recv(&mut self) -> Option<PeerEvent> {
            self.events_rx.next().now_or_never().flatten()
        }

        async fn invoke<M>(&mut self, message: M) -> Result<Option<Response>, Infallible>
        where
            M: crate::Message,
        {
            self.service
                .invoke(self.peer(), "test".to_string(), message, self.bus())
                .await
        }

        fn peer_id(&self) -> PeerId {
            self.descriptor.peer.id.clone()
        }

        fn peer(&self) -> Peer {
            self.descriptor.peer.clone()
        }

        fn bus(&self) -> Arc<NoopBus> {
            Arc::clone(&self.bus)
        }

        fn create_descriptor() -> PeerDescriptor {
            PeerDescriptor {
                peer: Peer::test(),
                subscriptions: vec![],
                is_persistent: true,
                timestamp_utc: Some(chrono::Utc::now()),
                has_debugger_attached: Some(false),
            }
        }

        fn create_descriptors(n: usize) -> Vec<PeerDescriptor> {
            Self::create_descriptors_with(n, |_| Self::create_descriptor())
        }

        fn create_descriptors_with(
            n: usize,
            create_fn: impl Fn(usize) -> PeerDescriptor,
        ) -> Vec<PeerDescriptor> {
            (0..n).into_iter().map(create_fn).collect()
        }

        fn create_subscription(message: &dyn Message) -> Subscription {
            MessageBinding::of_val(message).into()
        }

        fn create_subscription_for<M: MessageDescriptor + 'static>(
            messages: &[&dyn Message],
        ) -> SubscriptionsForType {
            let message_type = MessageTypeId::of::<M>().into_protobuf();
            let bindings = messages
                .iter()
                .map(|m| BindingKey::create(*m).into_protobuf())
                .collect();
            SubscriptionsForType {
                message_type,
                bindings,
            }
        }

        fn assert_event(event: Option<PeerEvent>, kind: PeerEventKind, peer: Peer) {
            assert!(event.is_some());
            let event = event.unwrap();

            assert_eq!(event.kind(), kind);
            assert_eq!(event.descriptor().peer().clone(), peer);
        }
    }

    #[tokio::test]
    async fn handle_registration() {
        let fixture = Fixture::new();
        let descriptors = Fixture::create_descriptors(5);
        fixture.client.handle_registration(RegisterPeerResponse {
            peers: descriptors.clone().into_protobuf(),
        });

        for descriptor in &descriptors {
            let peer = &descriptor.peer;

            assert_eq!(
                fixture.client.get_peer(&peer.id).as_ref().map(|p| p.peer()),
                Some(peer)
            );
        }
        assert_eq!(fixture.client.get_peer(&PeerId::new("Test.Peer.0")), None);
    }

    #[tokio::test]
    async fn handle_registration_does_not_raise_peer_started_event() {
        let mut fixture = Fixture::new();
        let descriptors = Fixture::create_descriptors(5);
        fixture.client.handle_registration(RegisterPeerResponse {
            peers: descriptors.into_protobuf(),
        });

        assert_eq!(fixture.try_recv().await, None);
    }

    #[tokio::test]
    async fn handle_registration_with_command_subscriptions() {
        let fixture = Fixture::new();
        let command_1 = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };
        let command_2 = RoutableCommand {
            name: "routable_command".into(),
            id: 0x0EFEF,
        };
        let command_3 = RoutableCommand {
            name: "routable_command".into(),
            id: 0xBADC0FFEE,
        };

        let peer_desc_1 = PeerDescriptor {
            peer: Peer::test(),
            subscriptions: vec![Fixture::create_subscription(&command_1)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };
        let peer_desc_2 = PeerDescriptor {
            peer: Peer::test(),
            subscriptions: vec![Fixture::create_subscription(&command_2)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };

        fixture.client.handle_registration(RegisterPeerResponse {
            peers: vec![
                peer_desc_1.clone().into_protobuf(),
                peer_desc_2.clone().into_protobuf(),
            ],
        });

        let peer_1 = fixture.client.get_peers_handling_message(&command_1);
        assert_eq!(peer_1, vec![peer_desc_1.peer]);
        let peer_2 = fixture.client.get_peers_handling_message(&command_2);
        assert_eq!(peer_2, vec![peer_desc_2.peer]);
        let peer_3 = fixture.client.get_peers_handling_message(&command_3);
        assert_eq!(peer_3, vec![]);
    }

    #[tokio::test]
    async fn handle_registration_with_event_subscriptions() {
        let fixture = Fixture::new();
        let event_1 = TestEvent {
            _outcome: "Passed".to_string(),
        };
        let event_2 = TestEvent {
            _outcome: "Failure".to_string(),
        };
        let subscription = Subscription::with_binding::<TestEvent>(BindingKey::empty());
        let descriptors = Fixture::create_descriptors_with(5, |_| PeerDescriptor {
            peer: Peer::test(),
            subscriptions: vec![subscription.clone()],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        });
        let peers: Vec<_> = descriptors.iter().cloned().map(|d| d.peer).collect();

        fixture.client.handle_registration(RegisterPeerResponse {
            peers: descriptors.clone().into_protobuf(),
        });

        let peers_1 = fixture.client.get_peers_handling_message(&event_1);
        assert_eq!(peers_1, peers);
        let peers_2 = fixture.client.get_peers_handling_message(&event_2);
        assert_eq!(peers_2, peers);
    }

    #[tokio::test]
    async fn handle_registration_with_routable_event_subscriptions() {
        let fixture = Fixture::new();
        let event_1 = RoutableTestEvent {
            outcome: "Passed".to_string(),
        };
        let event_2 = RoutableTestEvent {
            outcome: "Failure".to_string(),
        };
        let descriptors_1 = Fixture::create_descriptors_with(3, |_| {
            let subscription = Fixture::create_subscription(&event_1);
            PeerDescriptor {
                peer: Peer::test(),
                subscriptions: vec![subscription],
                is_persistent: true,
                timestamp_utc: None,
                has_debugger_attached: Some(false),
            }
        });

        let descriptors_2 = Fixture::create_descriptors_with(3, |_| {
            let subscription = Fixture::create_subscription(&event_2);
            PeerDescriptor {
                peer: Peer::test(),
                subscriptions: vec![subscription],
                is_persistent: true,
                timestamp_utc: None,
                has_debugger_attached: Some(false),
            }
        });
        let descriptor_peers_1 = descriptors_1.iter().map(|d| d.peer.clone()).collect_vec();
        let descriptor_peers_2 = descriptors_2.iter().map(|d| d.peer.clone()).collect_vec();
        let descriptors = descriptors_1
            .iter()
            .chain(descriptors_2.iter())
            .cloned()
            .collect_vec();

        fixture.client.handle_registration(RegisterPeerResponse {
            peers: descriptors.into_protobuf(),
        });

        let peers_1 = fixture.client.get_peers_handling_message(&event_1);
        assert_eq!(peers_1, descriptor_peers_1);
        let peers_2 = fixture.client.get_peers_handling_message(&event_2);
        assert_eq!(peers_2, descriptor_peers_2);
    }

    #[tokio::test]
    async fn handle_peer_started() {
        let mut fixture = Fixture::new();
        let descriptor = fixture.descriptor.clone();

        fixture
            .invoke(PeerStarted {
                descriptor: descriptor.clone().into_protobuf(),
            })
            .await
            .unwrap();

        let peer_descriptor = fixture.client.get_peer(&fixture.peer_id());
        let peer = peer_descriptor.as_ref().map(|p| p.peer());

        assert_eq!(peer, Some(&fixture.peer()));
        assert_eq!(fixture.client.get_peer(&PeerId::new("Test.Peer.0")), None);

        Fixture::assert_event(fixture.recv().await, PeerEventKind::Started, fixture.peer());
    }

    #[tokio::test]
    async fn handle_peer_stopped() {
        let mut fixture = Fixture::new();
        let descriptor = fixture.descriptor.clone();

        let peer = fixture.peer();
        let peer_id = fixture.peer_id();
        let timestamp_utc = chrono::Utc::now();

        fixture
            .service
            .invoke(
                fixture.peer(),
                "test".to_string(),
                PeerStarted {
                    descriptor: descriptor.clone().into_protobuf(),
                },
                fixture.bus(),
            )
            .await
            .unwrap();

        fixture
            .service
            .invoke(
                fixture.peer(),
                "test".to_string(),
                PeerStopped {
                    id: peer_id.clone(),
                    endpoint: Some(peer.endpoint.clone()),
                    timestamp_utc: Some(timestamp_utc.into_protobuf()),
                },
                fixture.bus(),
            )
            .await
            .unwrap();

        let peer_stopped = fixture.client.get_peer(&peer_id).map(|d| d.peer);
        assert_eq!(
            peer_stopped,
            Some(Peer {
                id: peer.id.clone(),
                endpoint: peer.endpoint.clone(),
                is_up: false,
                is_responding: false
            })
        );

        let peer = fixture.peer();

        Fixture::assert_event(fixture.recv().await, PeerEventKind::Started, peer.clone());
        Fixture::assert_event(
            fixture.recv().await,
            PeerEventKind::Stopped,
            peer.set_down().set_not_responding(),
        );
    }

    #[tokio::test]
    async fn handle_peer_started_after_peer_stopped_with_updated_subscriptions() {
        let mut fixture = Fixture::new();

        let command_1 = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };
        let command_2 = RoutableCommand {
            name: "routable_command".into(),
            id: 0x0EFEF,
        };
        let command_3 = RoutableCommand {
            name: "routable_command".into(),
            id: 0xBADC0FFEE,
        };

        let test_peer = Peer::test();
        let peer_desc_1 = PeerDescriptor {
            peer: test_peer.clone(),
            subscriptions: vec![Fixture::create_subscription(&command_1)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };
        let peer_desc_2 = PeerDescriptor {
            peer: test_peer.clone(),
            subscriptions: vec![Fixture::create_subscription(&command_2)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };

        fixture
            .invoke(PeerStarted {
                descriptor: peer_desc_1.clone().into_protobuf(),
            })
            .await
            .unwrap();

        fixture
            .invoke(PeerStopped {
                id: test_peer.id.clone(),
                endpoint: Some(test_peer.endpoint.clone()),
                timestamp_utc: None,
            })
            .await
            .unwrap();

        fixture
            .invoke(PeerStarted {
                descriptor: peer_desc_2.clone().into_protobuf(),
            })
            .await
            .unwrap();

        let peer_1 = fixture.client.get_peer(&test_peer.id).map(|p| p.peer);
        assert_eq!(
            peer_1,
            Some(Peer {
                id: test_peer.id.clone(),
                endpoint: test_peer.endpoint.clone(),
                is_up: true,
                is_responding: true
            })
        );
        let peers_1 = fixture.client.get_peers_handling_message(&command_1);
        assert_eq!(peers_1, vec![]);
        let peers_2 = fixture.client.get_peers_handling_message(&command_2);
        assert_eq!(peers_2, vec![test_peer.clone()]);
        let peers_3 = fixture.client.get_peers_handling_message(&command_3);
        assert_eq!(peers_3, vec![]);

        Fixture::assert_event(
            fixture.recv().await,
            PeerEventKind::Started,
            test_peer.clone(),
        );
        Fixture::assert_event(
            fixture.recv().await,
            PeerEventKind::Stopped,
            test_peer.clone().set_not_responding().set_down(),
        );
        Fixture::assert_event(
            fixture.recv().await,
            PeerEventKind::Started,
            test_peer.clone(),
        );
        assert_eq!(fixture.try_recv().await, None);
    }

    #[tokio::test]
    async fn handle_peer_subscriptions_for_types_updated() {
        let mut fixture = Fixture::new();
        let descriptor = fixture.descriptor.clone();

        let command_1 = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };
        let command_2 = RoutableCommand {
            name: "routable_command".into(),
            id: 0x0EFEF,
        };
        let subscription = Fixture::create_subscription_for::<RoutableCommand>(&[&command_1]);

        fixture
            .invoke(PeerStarted {
                descriptor: descriptor.clone().into_protobuf(),
            })
            .await
            .unwrap();

        let peer_id = fixture.peer_id();
        let now = chrono::Utc::now();

        fixture
            .invoke(PeerSubscriptionsForTypesUpdated {
                peer_id: peer_id.clone(),
                subscriptions: vec![subscription],
                timestamp_utc: now.into_protobuf(),
            })
            .await
            .unwrap();

        let peers_1 = fixture.client.get_peers_handling_message(&command_1);
        assert_eq!(peers_1, vec![fixture.peer()]);
        let peers_2 = fixture.client.get_peers_handling_message(&command_2);
        assert!(peers_2.is_empty());

        Fixture::assert_event(fixture.recv().await, PeerEventKind::Started, fixture.peer());
        Fixture::assert_event(fixture.recv().await, PeerEventKind::Updated, fixture.peer());
    }

    #[tokio::test]
    async fn handle_peer_subscriptions_for_types_updated_discard_outdated() {
        let mut fixture = Fixture::new();
        let descriptor = fixture.descriptor.clone();

        let command_1 = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };
        let command_2 = RoutableCommand {
            name: "routable_command".into(),
            id: 0x0EFEF,
        };
        let subscription_1 = Fixture::create_subscription_for::<RoutableCommand>(&[&command_1]);
        let subscription_2 = Fixture::create_subscription_for::<RoutableCommand>(&[&command_2]);

        fixture
            .invoke(PeerStarted {
                descriptor: descriptor.clone().into_protobuf(),
            })
            .await
            .unwrap();

        let peer_id = fixture.peer_id();
        let now = chrono::Utc::now();

        fixture
            .invoke(PeerSubscriptionsForTypesUpdated {
                peer_id: peer_id.clone(),
                subscriptions: vec![subscription_1],
                timestamp_utc: now.into_protobuf(),
            })
            .await
            .unwrap();

        fixture
            .invoke(PeerSubscriptionsForTypesUpdated {
                peer_id: peer_id.clone(),
                subscriptions: vec![subscription_2],
                timestamp_utc: (now - Duration::seconds(30)).into_protobuf(),
            })
            .await
            .unwrap();

        let peers_1 = fixture.client.get_peers_handling_message(&command_1);
        assert_eq!(peers_1, vec![fixture.peer()]);
        let peers_2 = fixture.client.get_peers_handling_message(&command_2);
        assert!(peers_2.is_empty());

        Fixture::assert_event(fixture.recv().await, PeerEventKind::Started, fixture.peer());
        Fixture::assert_event(fixture.recv().await, PeerEventKind::Updated, fixture.peer());
    }
}
