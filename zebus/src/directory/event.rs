use crate::PeerId;

#[derive(Clone, Debug)]
pub(crate) enum PeerEvent {
    /// A new peer [`PeerId`] has been started
    Started(PeerId),

    /// Peer [`PeerId`] has been stopped
    Stopped(PeerId),

    /// Peer [`PeerId`] has been updated
    Updated(PeerId),

    /// Peer [`PeerId`] has been decomissionned
    Decomissionned(PeerId),
}
