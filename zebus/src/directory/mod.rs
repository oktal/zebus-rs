pub(crate) mod commands;
pub(crate) mod descriptor;
pub(crate) mod event;
pub mod events;

mod client;
pub(crate) use client::{Client, Receiver};

pub use events::{PeerDecommissioned, PeerNotResponding, PeerResponding, PeerStarted, PeerStopped};

pub(crate) mod registration;
pub(crate) use registration::{Registration, RegistrationError};

pub use descriptor::PeerDescriptor;
