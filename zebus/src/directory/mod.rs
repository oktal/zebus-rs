pub(crate) mod commands;
pub(crate) mod descriptor;
pub(crate) mod event;
pub mod events;

mod client;
pub(crate) use client::Client;

use futures_core::Stream;
use std::{pin::Pin, sync::Arc};

pub use events::{PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped};

pub(crate) mod registration;
pub(crate) use registration::{Registration, RegistrationError};

pub use descriptor::PeerDescriptor;

use self::{commands::RegisterPeerResponse, event::PeerEvent};

use crate::{dispatch::InvokerService, Message, Peer, PeerId};

/// Alias for the [`Directory`] [`PeerEvent`] [`Stream`]
pub(crate) type EventStream = Pin<Box<dyn Stream<Item = PeerEvent> + Send + Sync + 'static>>;

/// Trait to read state from the directory
/// The Directory is where the state of the bus and the peers registered with the bus is stored
/// This component can be used to retrieve information about the state of the registered peers
pub trait DirectoryReader: Send + Sync + 'static {
    /// Get the [`Peer`] corresponding to a [`PeerId`]
    /// Returns `Some` if the peer exists and has been found or `None` otherwise
    fn get(&self, peer_id: &PeerId) -> Option<Peer>;

    /// Get the list of [`Peer`] peers handling a [`crate::Message`] message based on the
    /// subscriptions of the peers
    fn get_peers_handling(&self, message: &dyn Message) -> Vec<Peer>;
}

// TODO(oktal): can we relax `Sync` here ?
/// A description trait for a directory
pub(crate) trait Directory: DirectoryReader {
    /// Type of [`PeerEvent`] [`Stream`] that the directory will yield
    type EventStream: Stream<Item = PeerEvent> + Send + Sync + 'static;

    /// Type of [`Handler`] that will be used to handle commands and events related to the
    /// directory
    type Handler: InvokerService;

    /// Create a new instance of the directory
    fn new() -> Arc<Self>;

    /// Create a new subscription to the peer events stream
    fn subscribe(&self) -> Self::EventStream;

    /// Handle [`RegisterPeerResponse`] response from the directory server
    fn handle_registration(&self, response: RegisterPeerResponse);

    /// Create a new instance of a [`Self::Handler`]
    fn handler(&self) -> Self::Handler;
}
