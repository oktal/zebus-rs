use crate::{Command, Peer, PeerId};

pub struct CommandResult {
    error_code: i64,
}

pub struct CommandFuture(pub(crate) tokio::sync::oneshot::Receiver<CommandResult>);

pub trait Bus {
    type Err: std::fmt::Debug;

    fn configure(&mut self, peer_id: PeerId, environment: String) -> Result<(), Self::Err>;

    fn start(&mut self) -> Result<(), Self::Err>;
    fn stop(&mut self) -> Result<(), Self::Err>;

    fn send<C: Command>(&mut self, command: &C);
    fn send_to<C: Command + prost::Message>(
        &mut self,
        command: &C,
        peer: Peer,
    ) -> Result<CommandFuture, Self::Err>;
}
