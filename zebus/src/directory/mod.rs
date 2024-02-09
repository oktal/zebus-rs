pub(crate) mod binding;
mod client;
pub(crate) mod commands;
pub(crate) mod descriptor;
pub(crate) mod event;
pub mod events;
#[cfg(test)]
pub(crate) mod memory;
pub(crate) mod registration;

pub use binding::MessageBinding;
pub(crate) use client::Client;
pub use descriptor::PeerDescriptor;
pub use events::{PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped};
pub(crate) use registration::{Registration, RegistrationError};

use self::{commands::RegisterPeerResponse, event::PeerEvent};
use crate::{dispatch::InvokerService, Message, MessageDescriptor, Peer, PeerId};
use futures_core::Stream;
use std::{pin::Pin, sync::Arc};

/// Alias for the [`Directory`] [`PeerEvent`] [`Stream`]
pub(crate) type EventStream = Pin<Box<dyn Stream<Item = PeerEvent> + Send + Sync + 'static>>;

/// Trait to read state from the directory
/// The Directory is where the state of the bus and the peers registered with the bus is stored
/// This component can be used to retrieve information about the state of the registered peers
pub trait DirectoryReader: Send + Sync + 'static {
    /// Get the [`PeerDescriptor`] descriptor corresponding to a [`PeerId`]
    /// Returns `Some` if the peer exists and has been found or `None` otherwise
    fn get(&self, peer_id: &PeerId) -> Option<PeerDescriptor>;

    /// Get the list of [`Peer`] peers handling a [`Message`] with specifing [`crate::BindingKey`] binding
    fn get_peers_handling(&self, binding: &MessageBinding) -> Vec<Peer>;
}

/// Extension trait for [`DirectoryReader`]
pub trait DirectoryReaderExt: DirectoryReader {
    /// Get the list of [`Peer`] peers handling a [`Message`]
    fn get_peers_handling_message<M>(&self, msg: &M) -> Vec<Peer>
    where
        M: Message + MessageDescriptor + 'static,
    {
        self.get_peers_handling(&MessageBinding::of(msg))
    }

    fn get_peers_handling_message_val(&self, msg: &dyn Message) -> Vec<Peer> {
        self.get_peers_handling(&MessageBinding::of_val(msg))
    }
}

impl<D> DirectoryReaderExt for D where D: DirectoryReader + ?Sized {}

// TODO(oktal): can we relax `Sync` here ?
/// A description trait for a directory
pub(crate) trait Directory: Send + Sync + 'static {
    /// Type of [`PeerEvent`] [`Stream`] that the directory will yield
    type EventStream: Stream<Item = PeerEvent> + Unpin + Send + Sync + 'static;

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

    /// Create a [`DirectoryReader`] reader
    fn reader(&self) -> Arc<dyn DirectoryReader>;
}
