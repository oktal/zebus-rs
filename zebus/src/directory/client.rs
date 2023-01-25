//! A client to communicate with a a peer directory
use itertools::Itertools;
use tokio::sync::broadcast;

use crate::{
    core::MessagePayload,
    proto::{self, PeerDescriptor},
    routing::tree::PeerSubscriptionTree,
    transport::TransportMessage,
    BindingKey, Handler, Message, MessageType, Peer, PeerId,
};
use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::{Arc, Mutex},
};

use super::{
    commands::{PingPeerCommand, RegisterPeerResponse},
    event::PeerEvent,
    events::{PeerSubscriptionsForTypeUpdated, SubscriptionsForType},
    Directory, DirectoryReader, PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted,
    PeerStopped,
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

    fn get(&self, message_type: &MessageType, binding: &BindingKey) -> Vec<Peer> {
        match self.0.get(message_type) {
            Some(tree) => tree.get_peers(binding),
            None => vec![],
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
        index: &mut SubscriptionIndex,
        subscriptions: Vec<proto::Subscription>,
        timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let subscription_bindings = subscriptions
            .into_iter()
            .map(|s| (s.message_type_id, s.binding_key))
            .into_group_map();

        for (message_type, bindings) in subscription_bindings {
            let message_type = MessageType::from(message_type.full_name);
            let binding_keys = bindings.into_iter().map(Into::into).collect::<HashSet<_>>();
            self.set_message_subscriptions(index, message_type, binding_keys, timestamp_utc);
        }
    }

    fn set_subscriptions_for(
        &mut self,
        index: &mut SubscriptionIndex,
        subscriptions: Vec<SubscriptionsForType>,
        timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        for subscription in subscriptions {
            let message_type = MessageType::from(subscription.message_type);
            let binding_keys = subscription
                .bindings
                .into_iter()
                .map(Into::into)
                .collect::<HashSet<_>>();
            self.set_message_subscriptions(index, message_type, binding_keys, timestamp_utc);
        }
    }

    fn set_message_subscriptions(
        &mut self,
        index: &mut SubscriptionIndex,
        message_type: MessageType,
        binding_keys: HashSet<BindingKey>,
        timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
    ) {
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

struct PeerUpdate<'a> {
    id: &'a PeerId,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl PeerUpdate<'_> {
    fn raise(&self, event_fn: impl FnOnce(PeerId) -> PeerEvent) {
        if let Err(_) = self.events_tx.send(event_fn(self.id.clone())) {}
    }

    fn forget(self) {}
}

#[derive(Debug)]
struct Inner {
    subscriptions: SubscriptionIndex,
    peers: HashMap<PeerId, PeerEntry>,
    events_tx: broadcast::Sender<PeerEvent>,
}

impl Inner {
    fn new() -> Self {
        let (events_tx, _) = broadcast::channel(128);

        Self {
            subscriptions: SubscriptionIndex::new(),
            peers: HashMap::new(),
            events_tx,
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events_tx.subscribe()
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

        let index = &mut self.subscriptions;
        let timestamp_utc = timestamp_utc.and_then(|t| t.try_into().ok());
        peer_entry.set_subscriptions(index, subscriptions, timestamp_utc);

        PeerUpdate {
            id: &peer_entry.peer.id,
            events_tx: self.events_tx.clone(),
        }
    }

    fn set_subscriptions_for<'a>(
        &mut self,
        peer_id: &'a PeerId,
        subscriptions: Vec<SubscriptionsForType>,
        timestamp_utc: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Option<PeerUpdate<'a>> {
        let mut modified = false;
        self.peers.entry(peer_id.clone()).and_modify(|e| {
            if let Some(timestamp) = timestamp_utc {
                if let Some(entry) = e.checked_mut(timestamp) {
                    entry.set_subscriptions_for(
                        &mut self.subscriptions,
                        subscriptions,
                        timestamp_utc,
                    );
                    modified = true;
                }
            } else {
                e.set_subscriptions_for(&mut self.subscriptions, subscriptions, timestamp_utc);
                modified = true;
            }
        });

        modified.then_some(PeerUpdate {
            id: peer_id,
            events_tx: self.events_tx.clone(),
        })
    }

    fn get_peers_handling(&self, message: &dyn Message) -> Vec<Peer> {
        let message_type = MessageType::of_val(message);
        let binding_key = BindingKey::create(message);
        self.subscriptions.get(&message_type, &binding_key)
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
    type Response = ();

    fn handle(&mut self, message: PeerStarted) {
        self.add_or_update(message.descriptor)
            .raise(PeerEvent::Started);
    }
}

impl Handler<PeerStopped> for Inner {
    type Response = ();

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
    type Response = ();

    fn handle(&mut self, message: PeerDecommissioned) {
        let update = self.remove(&message.id);
        if let Some(update) = update {
            update.raise(PeerEvent::Decomissionned);
        }
    }
}

impl Handler<PeerNotResponding> for Inner {
    type Response = ();

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
    type Response = ();

    fn handle(&mut self, message: PeerResponding) {
        let update = self.update_with(&message.id, None, |e| {
            e.peer.is_responding = true;
        });

        if let Some(update) = update {
            update.raise(PeerEvent::Updated);
        }
    }
}

impl Handler<PeerSubscriptionsForTypeUpdated> for Inner {
    type Response = ();

    fn handle(&mut self, message: PeerSubscriptionsForTypeUpdated) {
        let timestamp_utc = message
            .timestamp_utc
            .try_into()
            .ok()
            .unwrap_or(chrono::Utc::now());
        let update = self.set_subscriptions_for(
            &message.peer_id,
            message.subscriptions,
            Some(timestamp_utc),
        );

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
    pub(crate) fn replay(&mut self, mut messages: Vec<TransportMessage>) -> Vec<TransportMessage> {
        messages.retain(|m| self.replay_message(m));
        messages
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
        try_replay!(PeerSubscriptionsForTypeUpdated);
        false
    }
}

impl DirectoryReader for Client {
    fn get(&self, peer_id: &PeerId) -> Option<Peer> {
        let inner = self.inner.lock().unwrap();
        inner.entry(peer_id, None).map(|e| e.peer.clone())
    }

    fn get_peers_handling(&self, message: &dyn Message) -> Vec<Peer> {
        let inner = self.inner.lock().unwrap();
        inner.get_peers_handling(message)
    }
}

impl Directory for Client {
    type EventStream = crate::sync::stream::BroadcastStream<PeerEvent>;
    type Handler = DirectoryHandler;

    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        })
    }

    fn subscribe(&self) -> Self::EventStream {
        let inner = self.inner.lock().unwrap();
        inner.subscribe().into()
    }

    fn handler(&self) -> Box<Self::Handler> {
        Box::new(DirectoryHandler {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl crate::Handler<RegisterPeerResponse> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: RegisterPeerResponse) {
        let mut inner = self.inner.lock().unwrap();
        for descriptor in message.peers {
            inner.add_or_update(descriptor).forget();
        }
    }
}

impl crate::Handler<PeerStarted> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerStarted) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PeerStopped> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerStopped) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PeerDecommissioned> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerDecommissioned) {
        println!("{message:?}")
    }
}

impl crate::Handler<PeerNotResponding> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerNotResponding) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PeerResponding> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerResponding) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

impl crate::Handler<PingPeerCommand> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, _message: PingPeerCommand) {
        println!("PING");
    }
}

impl crate::Handler<PeerSubscriptionsForTypeUpdated> for DirectoryHandler {
    type Response = ();

    fn handle(&mut self, message: PeerSubscriptionsForTypeUpdated) {
        let mut inner = self.inner.lock().unwrap();
        inner.handle(message);
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::proto::{prost, IntoProtobuf, Subscription};
    use crate::{Message, MessageDescriptor, MessageTypeId};

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
        client: Arc<Client>,
        handler: Box<DirectoryHandler>,
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
                client,
                handler,
                descriptor,
                events_rx,
            }
        }

        async fn try_recv_n(self, n: usize) -> Vec<Option<PeerEvent>> {
            use crate::sync::stream::StreamExt;
            self.events_rx.take_exact(n).collect().await
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

        fn create_descriptors_with(
            n: usize,
            create_fn: impl Fn(usize) -> PeerDescriptor,
        ) -> Vec<PeerDescriptor> {
            (0..n).into_iter().map(create_fn).collect()
        }

        fn create_subscription(message: &dyn Message) -> Subscription {
            let message_type_id = MessageTypeId::of_val(message).into_protobuf();
            let binding_key = BindingKey::create(message).into_protobuf();
            Subscription {
                message_type_id,
                binding_key,
            }
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
    }

    #[tokio::test]
    async fn handle_registration() {
        let mut fixture = Fixture::new();
        let descriptors = Fixture::create_descriptors(5);
        fixture.handler.handle(RegisterPeerResponse {
            peers: descriptors.clone(),
        });

        for descriptor in &descriptors {
            let peer = &descriptor.peer;

            assert_eq!(fixture.client.get(&peer.id), Some(peer.clone()));
        }
        assert_eq!(fixture.client.get(&PeerId::new("Test.Peer.0")), None);
    }

    #[tokio::test]
    async fn handle_registration_does_not_raise_peer_started_event() {
        let mut fixture = Fixture::new();
        let descriptors = Fixture::create_descriptors(5);
        fixture.handler.handle(RegisterPeerResponse {
            peers: descriptors.clone(),
        });

        assert_eq!(
            fixture.try_recv_n(5).await,
            vec![None, None, None, None, None]
        );
    }

    #[tokio::test]
    async fn handle_registration_with_command_subscriptions() {
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

        fixture.handler.handle(RegisterPeerResponse {
            peers: vec![peer_desc_1.clone(), peer_desc_2.clone()],
        });

        let peer_1 = fixture.client.get_peers_handling(&command_1);
        assert_eq!(peer_1, vec![peer_desc_1.peer]);
        let peer_2 = fixture.client.get_peers_handling(&command_2);
        assert_eq!(peer_2, vec![peer_desc_2.peer]);
        let peer_3 = fixture.client.get_peers_handling(&command_3);
        assert_eq!(peer_3, vec![]);
    }

    #[tokio::test]
    async fn handle_registration_with_event_subscriptions() {
        let mut fixture = Fixture::new();
        let event_1 = TestEvent {
            _outcome: "Passed".to_string(),
        };
        let event_2 = TestEvent {
            _outcome: "Failure".to_string(),
        };
        let subscription = Subscription {
            message_type_id: MessageTypeId::of::<TestEvent>().into_protobuf(),
            binding_key: BindingKey::empty().into_protobuf(),
        };
        let descriptors = Fixture::create_descriptors_with(5, |_| PeerDescriptor {
            peer: Peer::test(),
            subscriptions: vec![subscription.clone()],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        });
        let peers: Vec<_> = descriptors.iter().cloned().map(|d| d.peer).collect();

        fixture.handler.handle(RegisterPeerResponse {
            peers: descriptors.clone(),
        });

        let peers_1 = fixture.client.get_peers_handling(&event_1);
        assert_eq!(peers_1, peers);
        let peers_2 = fixture.client.get_peers_handling(&event_2);
        assert_eq!(peers_2, peers);
    }

    #[tokio::test]
    async fn handle_registration_with_routable_event_subscriptions() {
        let mut fixture = Fixture::new();
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

        fixture.handler.handle(RegisterPeerResponse {
            peers: descriptors.clone(),
        });

        let peers_1 = fixture.client.get_peers_handling(&event_1);
        assert_eq!(peers_1, descriptor_peers_1);
        let peers_2 = fixture.client.get_peers_handling(&event_2);
        assert_eq!(peers_2, descriptor_peers_2);
    }

    #[tokio::test]
    async fn handle_peer_started() {
        let mut fixture = Fixture::new();
        fixture.handler.handle(PeerStarted {
            descriptor: fixture.descriptor.clone(),
        });

        let peer_id = fixture.peer_id();

        assert_eq!(fixture.client.get(&fixture.peer_id()), Some(fixture.peer()));
        assert_eq!(fixture.client.get(&PeerId::new("Test.Peer.0")), None);
        assert_eq!(
            fixture.try_recv_n(1).await,
            vec![Some(PeerEvent::Started(peer_id))]
        );
    }

    #[tokio::test]
    async fn handle_peer_stopped() {
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

        let peer_id = fixture.peer_id();

        let peer_stopped = fixture.client.get(&peer_id);
        assert_eq!(
            peer_stopped,
            Some(Peer {
                id: peer.id.clone(),
                endpoint: peer.endpoint.clone(),
                is_up: false,
                is_responding: false
            })
        );

        let events = fixture.try_recv_n(2).await;
        assert_eq!(
            events,
            vec![
                Some(PeerEvent::Started(peer_id.clone())),
                Some(PeerEvent::Stopped(peer_id.clone()))
            ]
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

        let peer = Peer::test();
        let peer_desc_1 = PeerDescriptor {
            peer: peer.clone(),
            subscriptions: vec![Fixture::create_subscription(&command_1)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };
        let peer_desc_2 = PeerDescriptor {
            peer: peer.clone(),
            subscriptions: vec![Fixture::create_subscription(&command_2)],
            is_persistent: true,
            timestamp_utc: None,
            has_debugger_attached: Some(false),
        };

        fixture.handler.handle(PeerStarted {
            descriptor: peer_desc_1.clone(),
        });
        fixture.handler.handle(PeerStopped {
            id: peer.id.clone(),
            endpoint: Some(peer.endpoint.clone()),
            timestamp_utc: None,
        });
        fixture.handler.handle(PeerStarted {
            descriptor: peer_desc_2.clone(),
        });

        let peer_1 = fixture.client.get(&peer.id);
        assert_eq!(
            peer_1,
            Some(Peer {
                id: peer.id.clone(),
                endpoint: peer.endpoint.clone(),
                is_up: true,
                is_responding: true
            })
        );
        let peers_1 = fixture.client.get_peers_handling(&command_1);
        assert_eq!(peers_1, vec![]);
        let peers_2 = fixture.client.get_peers_handling(&command_2);
        assert_eq!(peers_2, vec![peer.clone()]);
        let peers_3 = fixture.client.get_peers_handling(&command_3);
        assert_eq!(peers_3, vec![]);

        assert_eq!(
            fixture.try_recv_n(4).await,
            vec![
                Some(PeerEvent::Started(peer.id.clone())),
                Some(PeerEvent::Stopped(peer.id.clone())),
                Some(PeerEvent::Started(peer.id.clone())),
                None,
            ]
        );
    }

    #[tokio::test]
    async fn handle_peer_subscriptions_for_type_updated() {
        let mut fixture = Fixture::new();
        let command_1 = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };
        let command_2 = RoutableCommand {
            name: "routable_command".into(),
            id: 0x0EFEF,
        };
        let subscription = Fixture::create_subscription_for::<RoutableCommand>(&[&command_1]);

        fixture.handler.handle(PeerStarted {
            descriptor: fixture.descriptor.clone(),
        });

        let peer_id = fixture.peer_id();
        let now = chrono::Utc::now();

        fixture.handler.handle(PeerSubscriptionsForTypeUpdated {
            peer_id: fixture.peer_id(),
            subscriptions: vec![subscription],
            timestamp_utc: now.into_protobuf(),
        });

        let peers_1 = fixture.client.get_peers_handling(&command_1);
        assert_eq!(peers_1, vec![fixture.peer()]);
        let peers_2 = fixture.client.get_peers_handling(&command_2);
        assert!(peers_2.is_empty());
        let events = fixture.try_recv_n(2).await;
        assert_eq!(
            events,
            vec![
                Some(PeerEvent::Started(peer_id.clone())),
                Some(PeerEvent::Updated(peer_id.clone()))
            ]
        );
    }

    #[tokio::test]
    async fn handle_peer_subscriptions_for_type_updated_discard_outdated() {
        let mut fixture = Fixture::new();
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

        fixture.handler.handle(PeerStarted {
            descriptor: fixture.descriptor.clone(),
        });

        let peer_id = fixture.peer_id();
        let now = chrono::Utc::now();

        fixture.handler.handle(PeerSubscriptionsForTypeUpdated {
            peer_id: fixture.peer_id(),
            subscriptions: vec![subscription_1],
            timestamp_utc: now.into_protobuf(),
        });

        fixture.handler.handle(PeerSubscriptionsForTypeUpdated {
            peer_id: fixture.peer_id(),
            subscriptions: vec![subscription_2],
            timestamp_utc: (now - Duration::seconds(30)).into_protobuf(),
        });

        let peers_1 = fixture.client.get_peers_handling(&command_1);
        assert_eq!(peers_1, vec![fixture.peer()]);
        let peers_2 = fixture.client.get_peers_handling(&command_2);
        assert!(peers_2.is_empty());
        assert_eq!(
            fixture.try_recv_n(3).await,
            vec![
                Some(PeerEvent::Started(peer_id.clone())),
                Some(PeerEvent::Updated(peer_id.clone())),
                None
            ]
        );
    }
}
