pub(crate) mod commands;
mod descriptor;
pub mod events;

mod client;
pub(crate) use client::Client;

pub use events::{PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped};

pub(crate) mod registration;
pub(crate) use registration::{Registration, RegistrationError};

pub use descriptor::PeerDescriptor;
