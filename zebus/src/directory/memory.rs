use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use tokio::sync::broadcast;

use crate::{
    dispatch::{RouteHandler, Router},
    inject::{self, State},
    BindingExpression, MessageDescriptor, Peer, PeerId, Subscription,
};

use super::{
    commands::{PingPeerCommand, RegisterPeerResponse},
    event::PeerEvent,
    events::PeerSubscriptionsForTypeUpdated,
    Directory, DirectoryReader, MessageBinding, PeerDecommissioned, PeerDescriptor,
    PeerNotResponding, PeerResponding, PeerStarted, PeerStopped,
};

/// State of the memory directory
pub(crate) struct MemoryDirectoryState {
    /// Sender channel for peer events
    events_tx: broadcast::Sender<PeerEvent>,

    /// Collection of messages that have been handled by the directory, indexed by their
    /// message type
    messages: HashMap<&'static str, Vec<Arc<dyn Any + Send + Sync>>>,

    /// Collection of peers that have been configured to handle a type of message
    peers: HashMap<PeerId, PeerDescriptor>,
}

impl MemoryDirectoryState {
    fn new() -> Self {
        let (events_tx, _events_rx) = broadcast::channel(128);
        Self {
            events_tx,
            messages: HashMap::new(),
            peers: HashMap::new(),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events_tx.subscribe()
    }

    /// Ad a `message` to the list of handled messages by the directory
    fn add_handled<M: MessageDescriptor + Send + Sync + 'static>(&mut self, message: M) {
        let entry = self.messages.entry(M::name()).or_insert(Vec::new());
        entry.push(Arc::new(message));
    }

    fn add_subscription_for(
        &mut self,
        peer: Peer,
        subscription: Subscription,
    ) -> &mut PeerDescriptor {
        let descriptor = self.peers.entry(peer.id.clone()).or_insert(PeerDescriptor {
            peer,
            subscriptions: vec![],
            is_persistent: true,
            timestamp_utc: Some(Utc::now()),
            has_debugger_attached: Some(false),
        });

        descriptor.subscriptions.push(subscription);
        descriptor
    }
}

/// A [`Directory`] that stores state in memory and has simplified
/// logic for test purposes
pub(crate) struct MemoryDirectory {
    state: Arc<Mutex<MemoryDirectoryState>>,
}

impl MemoryDirectory {
    /// Get a list of messages handled by the directory
    pub(crate) fn get_handled<M: MessageDescriptor + Send + Sync + 'static>(&self) -> Vec<Arc<M>> {
        let state = self.state.lock().unwrap();

        match state.messages.get(M::name()) {
            Some(entry) => entry
                .iter()
                .filter_map(|m| m.clone().downcast::<M>().ok())
                .collect(),
            None => vec![],
        }
    }

    /// Add a peer that should hande the `Message`
    pub(crate) fn add_peer_for<M: MessageDescriptor + BindingExpression + 'static>(
        &self,
        peer: Peer,
        descriptor_fn: impl FnOnce(&mut PeerDescriptor),
    ) -> &Self {
        self.add_subscription_for(peer, Subscription::any::<M>(), descriptor_fn);
        self
    }

    pub(crate) fn add_subscription_for(
        &self,
        peer: Peer,
        subscription: Subscription,
        descriptor_fn: impl FnOnce(&mut PeerDescriptor),
    ) {
        let mut state = self.state.lock().unwrap();
        let descriptor = state.add_subscription_for(peer, subscription);
        descriptor_fn(descriptor);
    }
}

impl DirectoryReader for MemoryDirectory {
    fn get_peer(&self, peer_id: &PeerId) -> Option<PeerDescriptor> {
        let state = self.state.lock().unwrap();
        state.peers.get(peer_id).cloned()
    }

    fn get_peers_handling(&self, binding: &MessageBinding) -> Vec<Peer> {
        let state = self.state.lock().unwrap();

        println!("Getting peers handling {binding:?}");
        println!("{:#?}", state.peers);

        state
            .peers
            .values()
            .filter_map(|descriptor| {
                descriptor
                    .handles(binding)
                    .then_some(descriptor.peer.clone())
            })
            .collect()
    }
}

async fn peer_started(
    msg: PeerStarted,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn peer_stopped(
    msg: PeerStopped,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn peer_decommissioned(
    msg: PeerDecommissioned,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn peer_not_responding(
    msg: PeerNotResponding,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn peer_responding(
    msg: PeerResponding,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn ping_peer(
    msg: PingPeerCommand,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

async fn peer_subscriptions_for_type_updated(
    msg: PeerSubscriptionsForTypeUpdated,
    inject::State(state): State<Arc<Mutex<MemoryDirectoryState>>>,
) {
    state.lock().unwrap().add_handled(msg)
}

impl Directory for MemoryDirectory {
    type EventStream = crate::sync::stream::BroadcastStream<PeerEvent>;
    type Handler = Router<Arc<Mutex<MemoryDirectoryState>>>;

    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(MemoryDirectoryState::new())),
        })
    }
    fn subscribe(&self) -> Self::EventStream {
        let state = self.state.lock().unwrap();
        state.subscribe().into()
    }

    fn handle_registration(&self, response: RegisterPeerResponse) {
        let mut state = self.state.lock().unwrap();
        state.add_handled(response);
    }

    fn handler(&self) -> Self::Handler {
        Router::with_state(Arc::clone(&self.state))
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
