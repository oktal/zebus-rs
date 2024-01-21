use futures_core::{stream::BoxStream, Stream};
use futures_util::{pin_mut, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    bus::{BusEvent, CommandResult},
    core::MessagePayload,
    directory::event::PeerEvent,
    proto::{FromProtobuf, IntoProtobuf},
    sync::stream::EventStream,
    transport::{
        MessageExecutionCompleted, SendContext, Transport, TransportExt, TransportMessage,
    },
    BusConfiguration, CommandError, MessageId, Peer,
};

use super::{
    command::StartMessageReplayCommand,
    event::{ReplayEvent, TryFromReplayEventError},
    PersistenceError, PersistenceEvent, PersistenceRequest,
};

enum State {
    Init {
        peers: Vec<Peer>,
    },

    Registered,

    PersistenceReady {
        peers: Vec<Peer>,
    },

    ReplayPhase {
        command_id: uuid::Uuid,
        id: uuid::Uuid,

        queue: Vec<TransportMessage>,
        replayed: usize,
    },

    SafetyPhase {
        id: uuid::Uuid,
        queue: Vec<TransportMessage>,
    },

    NormalPhase {
        queue: Vec<TransportMessage>,
    },
}

struct PersistenceService<S, T> {
    bus_events_rx: S,
    inner: T,
    forward_tx: broadcast::Sender<TransportMessage>,
    reqs_rx: mpsc::Receiver<PersistenceRequest>,
    events_tx: EventStream<PersistenceEvent>,
    shutdown: CancellationToken,
}

impl<S, T> PersistenceService<S, T>
where
    S: Stream<Item = BusEvent>,
    T: Transport,
{
    async fn run(self) -> Result<(), PersistenceError> {
        let mut inner_rx = self
            .inner
            .subscribe()
            .map_err(|e| PersistenceError::Transport(e.into()))?;
        let stream = self.bus_events_rx;

        let forward_tx = self.forward_tx;
        let mut reqs_rx = self.reqs_rx;
        let mut inner = self.inner;

        pin_mut!(stream);

        let mut state = State::Init { peers: Vec::new() };

        loop {
            tokio::select! {
               // We have been cancelled
               _ = self.shutdown.cancelled() => break Ok(()),

               // We received a bus event
               Some(event) = stream.next() => {
                    match event {
                        // Bus succesfully registered to the directory
                        BusEvent::Registered(directory_peers) => {
                            state = if let State::Init { mut peers } = state {
                                // Find persistence service peers
                                peers.extend(
                                    directory_peers
                                        .iter()
                                        .filter(|p| p.id.is_persistence() && p.is_up && p.is_responding)
                                        .cloned(),
                                );

                                // If the persistence is up and running, switch to the
                                // `PersistenceReady` state
                                if !peers.is_empty() {
                                    debug!("discovered persistence peers {peers:?}");
                                    State::PersistenceReady { peers }
                                } else {
                                    // We do not have a persistence peer available yet, switch to
                                    // the `Registered` state and wait for the persistence to start
                                    State::Registered
                                }

                            } else {
                                state
                            }

                        },
                        BusEvent::Peer(PeerEvent::Started(peer)) => {
                            state = match state {
                                State::Init { mut peers } => {
                                    // A persistence service peer started but we are still in the
                                    // initialization stage, add it to our list of persistence peers
                                    if peer.id.is_persistence() && peer.is_up {
                                        peers.push(peer)
                                    }

                                    State::Init { peers }
                                },
                                State::Registered => {
                                    // If we are registered and the peer that started is a
                                    // persistence service peer, switch to the `PersistenceReady`
                                    // state.
                                    // Otherwise, keep waiting for the persistence service to start
                                    if peer.id.is_persistence() && peer.is_up {
                                        debug!("discovered persistence peer {peer}");
                                        State::PersistenceReady { peers: vec![peer] }
                                    } else {
                                        State::Registered
                                    }
                                },
                                s => s,

                            }

                        },
                        _ => {}

                    }
                }

                // We received a message from the inner transport layer
                Some(message) = inner_rx.next() => {
                    state = match state {
                        // Replay phase
                        State::ReplayPhase { command_id, id, queue, replayed } => {
                            // We are in a replay phase, attempt to handle the message as a
                            // `ReplayEvent` from the persistence
                            match ReplayEvent::try_from(message) {
                                Ok(replay_event) => match replay_event {
                                    // Message replayed
                                    ReplayEvent::MessageReplayed(msg) => {
                                        let replay_id = msg.replay_id.to_uuid();

                                        debug!("replaying message with id {replay_id}");

                                        if replayed == 0 {
                                            let _ = self.events_tx.send(PersistenceEvent::ReplayStarted);
                                        }

                                        if replay_id != id {
                                            return Err(PersistenceError::ConflictingReplay {
                                                replay_id: id,
                                                conflicting_id: replay_id,
                                            });
                                        } else {
                                            // In the replay phase, forward a replayed message
                                            // directly
                                            let _ = forward_tx.send(msg.message);


                                            // Stay in replay phase
                                            State::ReplayPhase {
                                                command_id,
                                                id,
                                                queue,
                                                replayed: replayed + 1,
                                            }
                                        }
                                    }
                                    // Replay phase ended
                                    ReplayEvent::ReplayPhaseEnded(msg) => {
                                        let replay_id = msg.replay_id.to_uuid();
                                        if replay_id != id {
                                            return Err(PersistenceError::ConflictingReplay {
                                                replay_id: id,
                                                conflicting_id: replay_id,
                                            });
                                        } else {
                                            info!("switching to safety phase");

                                            let _ = self.events_tx.send(PersistenceEvent::SafetyStarted);

                                            // Switch to safety phase
                                            State::SafetyPhase {
                                                id,
                                                queue,
                                            }
                                        }
                                    }
                                    // We should not receive a `SafetyPhaseEnded` event in a replay
                                    // phase
                                    ReplayEvent::SafetyPhaseEnded(_msg) => return Err(PersistenceError::InvalidPhase),
                                },
                                Err(e) => match e {
                                    // Failed to decode incomming message
                                    TryFromReplayEventError::Decode(e) => return Err(PersistenceError::Decode(e)),

                                    // Incomming message is not a `ReplayEvent`
                                    TryFromReplayEventError::Other(msg) => {

                                        // Check if we received the `MessageExecutionCompleted`
                                        // from the `StartMessageReplayCommand`
                                        if let Some(message_execution_completed) = msg.decode_as::<MessageExecutionCompleted>() {
                                            match message_execution_completed {
                                                Ok(message_execution_completed) => {
                                                    let msg_id = MessageId::from_protobuf(message_execution_completed.command_id);
                                                    if msg_id.value() == command_id {
                                                        let result: CommandResult = message_execution_completed.into();

                                                        // We received a `MessageExecutionCompleted` for the `StartMessageReplayCommand` with
                                                        // an error response
                                                        if let Err(CommandError::Command { code, message }) = result {
                                                            error!("failed to start replay: error code ({code}) ({})", message.unwrap_or(String::new()));
                                                        }

                                                    }
                                                    else {
                                                        // Forward the message
                                                        let _ = forward_tx.send(msg);
                                                    }
                                                },
                                                Err(e) => error!("error decoding `MessageExecutionCompleted`: {e}")
                                            }
                                        }
                                        else
                                        {
                                            // TODO(oktal): only forward infrastructure messages
                                            // Forward the message
                                            let _ = forward_tx.send(msg);
                                        }

                                        // Stay in replay phase
                                        State::ReplayPhase { command_id, id, queue, replayed }
                                    }
                                }
                            }
                        }

                        // Safety phase
                        State::SafetyPhase { id, mut queue,  } => {
                            // We are in a safety phase, attempt to handle the message as a
                            // `ReplayEvent` from the persistence
                            match ReplayEvent::try_from(message) {
                                Ok(replay_event) => match replay_event {
                                    // Message replayed
                                    ReplayEvent::MessageReplayed(msg) => {
                                        let replay_id = msg.replay_id.to_uuid();

                                        debug!("forwarding message with id {replay_id}");

                                        if replay_id != id {
                                            return Err(PersistenceError::ConflictingReplay {
                                                replay_id: id,
                                                conflicting_id: replay_id,
                                            });
                                        } else {
                                            // We received a message to replay from the
                                            // persistence while we are in the safety phase,
                                            // queue it instead of forwarding it directly
                                            queue.push(msg.message);

                                            // Stay in safety phase
                                            State::SafetyPhase {
                                                id,
                                                queue,
                                            }
                                        }
                                    }

                                    // We should not receive a `ReplayPhaseEnded` while we are in
                                        // safety phase
                                    ReplayEvent::ReplayPhaseEnded(_msg) => return Err(PersistenceError::InvalidPhase),

                                    // Safety phase ended
                                    ReplayEvent::SafetyPhaseEnded(msg) => {
                                        let replay_id = msg.replay_id.to_uuid();


                                        if replay_id != id {
                                            return Err(PersistenceError::ConflictingReplay {
                                                replay_id: id,
                                                conflicting_id: replay_id,
                                            });
                                        } else {
                                            info!("switching to normal phase");

                                            let _ = self.events_tx.send(PersistenceEvent::Normal);

                                            // Switch to normal phase
                                            State::NormalPhase {
                                                queue,
                                            }
                                        }

                                    }
                                },
                                Err(e) => match e {
                                    // Failed to decode incomming message
                                    TryFromReplayEventError::Decode(e) => return Err(PersistenceError::Decode(e)),

                                    // Incomming message is not a replay event
                                    TryFromReplayEventError::Other(msg) => {
                                        // Forward the message right away instead of queueing
                                        // it
                                        let _ = forward_tx.send(msg);

                                        // Stay in safety phase
                                        State::SafetyPhase { id, queue  }
                                    }
                                }
                            }

                        }
                        s => {
                            // TODO(oktal): only forward infrastructure messages
                            let _ = forward_tx.send(message);
                            s
                        }
                    }
                }

                // We received a persistence request
                Some(req) = reqs_rx.recv() => {
                    match state {
                        State::Init { .. } | State::Registered | State::PersistenceReady { .. } | State::NormalPhase { .. } => {
                            inner.send(req.peers.into_iter(), req.message, SendContext::default())
                                .map_err(|e| PersistenceError::Transport(e.into()))?
                                .await
                                .map_err(|e| PersistenceError::Transport(e.into()))?;
                        },
                        State::ReplayPhase { ref mut queue, .. } | State::SafetyPhase { ref mut queue, .. } => queue.push(req.message),

                    }
                }
            }

            state =
                match state {
                    // We are ready to start the replay sequence
                    State::PersistenceReady { mut peers } => {
                        let peer = inner
                            .peer()
                            .map_err(|e| PersistenceError::Transport(e.into()))?;
                        let environment = inner
                            .environment()
                            .map_err(|e| PersistenceError::Transport(e.into()))?;
                        let replay_id = uuid::Uuid::new_v4();
                        let cmd = StartMessageReplayCommand {
                            replay_id: replay_id.into_protobuf(),
                        };

                        let (command_id, message) =
                            TransportMessage::create(&peer, environment.to_string(), &cmd);

                        debug!("starting replay with id {replay_id}");

                        inner
                            .send(
                                std::iter::once(peers.pop().expect(
                                    "we should have at least one available persistence peer",
                                )),
                                message,
                                SendContext::default(),
                            )
                            .map_err(|e| PersistenceError::Transport(e.into()))?
                            .await
                            .map_err(|e| PersistenceError::Transport(e.into()))?;

                        State::ReplayPhase {
                            command_id,
                            id: replay_id,
                            queue: Vec::new(),
                            replayed: 0,
                        }
                    }
                    s => s,
                };

            if let State::NormalPhase { ref mut queue, .. } = state {
                // If we just switched to normal phase, forward the incomming messages
                // that were received and queued during the safety phase
                for msg in queue.drain(..) {
                    let _ = forward_tx.send(msg);
                }
            }
        }
    }
}

struct TransientService<T> {
    inner: T,
    forward_tx: broadcast::Sender<TransportMessage>,
    reqs_rx: mpsc::Receiver<PersistenceRequest>,
    shutdown: CancellationToken,
}

impl<T> TransientService<T>
where
    T: Transport,
{
    async fn run(mut self) -> Result<(), PersistenceError> {
        let mut inner_rx = self
            .inner
            .subscribe()
            .map_err(|e| PersistenceError::Transport(e.into()))?;

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break Ok(()),

                Some(msg) = inner_rx.next() => {
                    let _ = self.forward_tx.send(msg);
                },

                Some(req) = self.reqs_rx.recv() => {
                    match self.inner.send(req.peers.into_iter(), req.message, SendContext::default()) {
                        Ok(fut) => if let Err(e) = fut.await {
                            error!("error sending message: {e}")
                        },
                        Err(e) => error!("error sending message: {e}")
                    }

                }
            }
        }
    }
}

pub(super) fn spawn<T, S>(
    configuration: &BusConfiguration,
    bus_events_rx: S,
    inner: T,
    forward_tx: broadcast::Sender<TransportMessage>,
    shutdown: CancellationToken,
) -> (
    mpsc::Sender<PersistenceRequest>,
    BoxStream<'static, PersistenceEvent>,
    super::future::StopFuture,
)
where
    S: Stream<Item = BusEvent> + Send + 'static,
    T: Transport,
{
    let (tx, rx) = mpsc::channel(128);

    if configuration.is_persistent {
        let events_tx = EventStream::<PersistenceEvent>::new(16);
        let events_rx = events_tx.stream();

        let service = PersistenceService {
            bus_events_rx,
            inner,
            forward_tx,
            reqs_rx: rx,
            events_tx,
            shutdown,
        };

        let fut = super::future::StopFuture::spawn(service.run());
        (tx, events_rx.boxed(), fut)
    } else {
        let service = TransientService {
            inner,
            forward_tx,
            reqs_rx: rx,
            shutdown,
        };

        let fut = super::future::StopFuture::spawn(service.run());

        let events_rx = futures_util::stream::iter([
            PersistenceEvent::ReplayStarted,
            PersistenceEvent::SafetyStarted,
            PersistenceEvent::Normal,
        ]);
        (tx, events_rx.boxed(), fut)
    }
}
