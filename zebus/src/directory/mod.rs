mod peer_descriptor;
mod register_peer_command;
mod register_peer_response;
mod registration;

pub(crate) use registration::{register, Registration, RegistrationError};

pub use peer_descriptor::PeerDescriptor;
use register_peer_command::RegisterPeerCommand;
pub(crate) use register_peer_response::RegisterPeerResponse;
