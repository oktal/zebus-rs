use crate::{Command, Peer, PeerId};

pub struct CommandResult {
    error_code: i64,
}

pub struct CommandFuture(pub(crate) tokio::sync::oneshot::Receiver<CommandResult>);

/// A Bus
pub trait Bus {
    /// Error type associated with the bus
    type Err: std::error::Error;

    /// Configure the bus with the provided [`PeerId`] `peer_id` and `environment`
    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err>;

    /// Start the bus
    fn start(&mut self) -> Result<(), Self::Err>;

    /// Stop the bus
    fn stop(&mut self) -> Result<(), Self::Err>;

    /// Send a [`Command`] to the handling [`Peer`]
    fn send<C: Command>(&mut self, command: &C);

    /// Send a [`Command`] to a particular [`Peer`]
    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: &C,
        peer: Peer,
    ) -> Result<CommandFuture, Self::Err>;
}
