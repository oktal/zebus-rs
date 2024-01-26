use crate::{core::MessagePayload, message_id::proto, proto::bcl, transport::TransportMessage};

pub(super) enum TryFromReplayEventError {
    /// [`TransportMessage`] is not a [`ReplayEvent`]
    Other(TransportMessage),

    /// Failed to decode [`ReplayEvent`]
    Decode(prost::DecodeError),
}

impl From<prost::DecodeError> for TryFromReplayEventError {
    fn from(value: prost::DecodeError) -> Self {
        Self::Decode(value)
    }
}

#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence", transient)]
pub(super) struct MessageHandled {
    #[prost(message, required, tag = 1)]
    pub id: proto::MessageId,
}

pub(super) enum ReplayEvent {
    MessageReplayed(MessageReplayed),
    ReplayPhaseEnded(ReplayPhaseEnded),
    SafetyPhaseEnded(SafetyPhaseEnded),
}

impl TryFrom<TransportMessage> for ReplayEvent {
    type Error = TryFromReplayEventError;

    fn try_from(value: TransportMessage) -> Result<Self, Self::Error> {
        if let Some(message_replayed) = value.decode_as::<MessageReplayed>() {
            Ok(Self::MessageReplayed(message_replayed?))
        } else if let Some(replay_phase_ended) = value.decode_as::<ReplayPhaseEnded>() {
            Ok(Self::ReplayPhaseEnded(replay_phase_ended?))
        } else if let Some(safety_phase_ended) = value.decode_as::<SafetyPhaseEnded>() {
            Ok(Self::SafetyPhaseEnded(safety_phase_ended?))
        } else {
            Err(TryFromReplayEventError::Other(value))
        }
    }
}

#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence", transient)]
pub struct MessageReplayed {
    #[prost(message, required, tag = 1)]
    pub replay_id: bcl::Guid,

    #[prost(message, required, tag = 2)]
    pub message: TransportMessage,
}

#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence", transient)]
pub struct ReplayPhaseEnded {
    #[prost(message, required, tag = 1)]
    pub replay_id: bcl::Guid,
}

#[derive(prost::Message, crate::Event, Clone)]
#[zebus(namespace = "Abc.Zebus.Persistence", transient)]
pub struct SafetyPhaseEnded {
    #[prost(message, required, tag = 1)]
    pub replay_id: bcl::Guid,
}
