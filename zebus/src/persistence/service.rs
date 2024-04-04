use std::{collections::HashSet, sync::Arc};

use futures_core::{stream::BoxStream, Stream};
use futures_util::{pin_mut, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    bus::{BusEvent, CommandResult},
    core::MessagePayload,
    directory::{event::PeerEvent, DirectoryReader, PeerDescriptor},
    persistence::is_persistence_peer,
    proto::{FromProtobuf, IntoProtobuf},
    sync::stream::EventStream,
    transport::{
        MessageExecutionCompleted, OriginatorInfo, SendContext, Transport, TransportMessage,
    },
    BusConfiguration, CommandError, MessageExt, MessageId, Peer, PeerId,
};

use super::{
    command::{PersistenceStopping, PersistenceStoppingAck, StartMessageReplayCommand},
    event::{ReplayEvent, TryFromReplayEventError},
    PersistenceError, PersistenceEvent, PersistenceRequest,
};

/// Represents a message to send to the persistence
enum PersistenceMessage {
    /// Special command to persist a message
    Persist {
        /// Original message to persist
        message: TransportMessage,

        /// List of persistent target peers for the message to persist
        persistent_peer_ids: Vec<PeerId>,
    },

    /// Any other message to send to the persistence
    Other(TransportMessage),
}

impl From<PersistenceMessage> for TransportMessage {
    fn from(value: PersistenceMessage) -> Self {
        match value {
            PersistenceMessage::Persist {
                mut message,
                persistent_peer_ids,
            } => {
                message.persistent_peer_ids = persistent_peer_ids;
                message
            }
            PersistenceMessage::Other(message) => message,
        }
    }
}

enum State {
    Init {
        peers: Vec<Peer>,
        descriptor: Option<PeerDescriptor>,

        tx_queue: Vec<PersistenceMessage>,
    },

    Registered {
        descriptor: PeerDescriptor,
        tx_queue: Vec<PersistenceMessage>,
    },

    PersistenceReady {
        peers: Vec<Peer>,
        descriptor: PeerDescriptor,
        tx_queue: Vec<PersistenceMessage>,
    },

    ReplayPhase {
        command_id: uuid::Uuid,
        id: uuid::Uuid,

        descriptor: PeerDescriptor,
        peers: Vec<Peer>,

        rx_queue: Vec<TransportMessage>,
        replayed: HashSet<MessageId>,
    },

    SafetyPhase {
        descriptor: PeerDescriptor,
        peers: Vec<Peer>,
        id: uuid::Uuid,
        replayed: HashSet<MessageId>,
    },

    NormalPhase {
        peers: Vec<Peer>,
    },
}

struct PersistenceService<T> {
    directory: Arc<dyn DirectoryReader>,
    peer: Peer,
    environment: String,
    inner: T,
    forward_tx: broadcast::Sender<TransportMessage>,
    reqs_rx: mpsc::Receiver<PersistenceRequest>,
    events_tx: EventStream<PersistenceEvent>,
    shutdown: CancellationToken,
}

impl<T> PersistenceService<T>
where
    T: Transport,
{
    async fn run<S>(mut self, bus_events: S) -> Result<(), PersistenceError>
    where
        S: Stream<Item = BusEvent>,
    {
        let mut inner_rx = self
            .inner
            .subscribe()
            .map_err(|e| PersistenceError::Transport(e.into()))?;

        pin_mut!(bus_events);

        let mut state = State::Init {
            peers: Vec::new(),
            descriptor: None,
            tx_queue: Vec::new(),
        };

        loop {
            tokio::select! {
               // We have been cancelled
               _ = self.shutdown.cancelled() => break Ok(()),

               // We received a bus event
               Some(event) = bus_events.next() => {
                    state = self.handle_event(state, event).await;
               }

                // We received a message from the inner transport layer
                Some(message) = inner_rx.next() => {
                    state = match self.handle_transport_message(message, state) {
                        Ok(state) => state,
                        Err((state, err)) => {
                            warn!("failed to handle transport message: {err}");
                            state
                        },
                    };
                }

                // We received a request to send a message
                Some(req) = self.reqs_rx.recv() => {
                    if let Err(err) = self.persist_message(req, &mut state).await {
                        warn!("failed to persist message: {err}");
                    }
                }
            }

            state = match state {
                // We are ready to start the replay sequence
                State::PersistenceReady {
                    peers,
                    descriptor,
                    tx_queue,
                } => {
                    // Create StartMessageReplayCommand
                    let replay_id = uuid::Uuid::new_v4();
                    let cmd = StartMessageReplayCommand {
                        replay_id: replay_id.into_protobuf(),
                    };

                    let (command_id, message) =
                        TransportMessage::create(&self.peer, self.environment.to_string(), &cmd);

                    let persistence_peer = peers
                        .last()
                        .expect("we should have at least one availablle persistence peer");

                    debug!("starting replay {replay_id} with {persistence_peer}");

                    // Start replay by sending StartMessageReplayCommand
                    self.send(
                        std::iter::once(persistence_peer.clone()),
                        message,
                        SendContext::Empty,
                    )
                    .await?;

                    // Drain the queue for any message waiting to be persisted
                    for message in tx_queue {
                        if let Err(e) = self
                            .send(
                                std::iter::once(persistence_peer.clone()),
                                message,
                                SendContext::Empty,
                            )
                            .await
                        {
                            // TODO(oktal): log message type
                            error!("failed to persist message: {e}",);
                        }
                    }

                    // Switch to ReplayPhase
                    State::ReplayPhase {
                        peers,
                        command_id,
                        id: replay_id,
                        descriptor,
                        rx_queue: Vec::new(),
                        replayed: HashSet::new(),
                    }
                }
                s => s,
            };
        }
    }

    async fn handle_event(&mut self, mut state: State, event: BusEvent) -> State {
        match event {
            // Bus is about to register to the directory
            BusEvent::Registering(desc) => {
                if let State::Init {
                    ref mut descriptor, ..
                } = state
                {
                    *descriptor = Some(desc);
                }

                state
            }

            // Bus succesfully registered to the directory
            BusEvent::Registered(directory_peers) => {
                if let State::Init {
                    mut peers,
                    mut descriptor,
                    tx_queue,
                } = state
                {
                    // Find persistence service peers
                    peers.extend(
                        directory_peers
                            .iter()
                            .filter(|p| {
                                is_persistence_peer(p) && p.peer.is_up && p.peer.is_responding
                            })
                            .map(|p| p.peer.clone()),
                    );

                    let descriptor = descriptor.take().expect(
                        "a PeerDescriptor should have been received through the BusEvent::Registering event",
                    );

                    // If the persistence is up and running, switch to the
                    // `PersistenceReady` state
                    if !peers.is_empty() {
                        State::PersistenceReady {
                            peers,
                            descriptor,
                            tx_queue,
                        }
                    } else {
                        // We do not have a persistence peer available yet, switch to
                        // the `Registered` state and wait for the persistence to start
                        State::Registered {
                            descriptor,
                            tx_queue,
                        }
                    }
                } else {
                    state
                }
            }
            BusEvent::Peer(PeerEvent::Started(peer)) => {
                match state {
                    State::Init {
                        mut peers,
                        descriptor,
                        tx_queue,
                    } => {
                        // A persistence service peer started but we are still in the
                        // initialization stage, add it to our list of persistence peers
                        if is_persistence_peer(&peer) && peer.peer.is_up {
                            peers.push(peer.peer)
                        }

                        State::Init {
                            peers,
                            descriptor,
                            tx_queue,
                        }
                    }
                    State::Registered {
                        descriptor,
                        tx_queue,
                    } => {
                        // If we are registered and the peer that started is a
                        // persistence service peer, switch to the `PersistenceReady`
                        // state.
                        // Otherwise, keep waiting for the persistence service to start
                        if is_persistence_peer(&peer) && peer.peer.is_up {
                            debug!("discovered persistence peer {}", peer.peer);
                            State::PersistenceReady {
                                peers: vec![peer.peer],
                                descriptor,
                                tx_queue,
                            }
                        } else {
                            State::Registered {
                                descriptor,
                                tx_queue,
                            }
                        }
                    }
                    s => s,
                }
            }

            BusEvent::MessageHandled { id, persisted, .. } => {
                // Ack the message to the persistence if the message was persisted
                if let (Some(message_id), true) = (id, persisted) {
                    let message_handled = super::event::MessageHandled {
                        id: message_id.into_protobuf(),
                    };

                    let (_, message) = TransportMessage::create(
                        &self.peer,
                        self.environment.to_string(),
                        &message_handled,
                    );

                    match &mut state {
                        State::Init { tx_queue, .. }
                        | State::Registered { tx_queue, .. }
                        | State::PersistenceReady { tx_queue, .. } => {
                            tx_queue.push(PersistenceMessage::Other(message));
                        }
                        State::ReplayPhase { peers, .. }
                        | State::SafetyPhase { peers, .. }
                        | State::NormalPhase { peers, .. } => {
                            let persistence_peer = peers
                                .first()
                                .expect("we should have at least one availablle persistence peer");

                            if let Err(e) = self
                                .send(
                                    std::iter::once(persistence_peer.clone()),
                                    message,
                                    SendContext::default(),
                                )
                                .await
                            {
                                error!("failed to ack message to persistence: {e}");
                            }
                        }
                    }
                }
                state
            }

            _ => state,
        }
    }

    fn handle_persistence_stopping(
        &mut self,
        originator: OriginatorInfo,
        stopping: Result<PersistenceStopping, prost::DecodeError>,
    ) {
        if let Err(e) = stopping {
            warn!("failed to decode `PersistenceStopping` message: {e}");
        }

        let (_id, ack) =
            PersistenceStoppingAck {}.as_transport(&self.peer, self.environment.clone());

        if let Err(e) = self
            .inner
            .send(std::iter::once(originator.into()), ack, SendContext::Empty)
        {
            warn!("failed to send `PersistenceStoppingAck`: {e}");
        }
    }

    fn handle_transport_message(
        &mut self,
        message: TransportMessage,
        state: State,
    ) -> Result<State, (State, PersistenceError)> {
        if let Some(stopping) = message.decode_as::<PersistenceStopping>() {
            self.handle_persistence_stopping(message.originator, stopping);
            return Ok(state);
        }

        match state {
            // Replay phase
            State::ReplayPhase {
                peers,
                command_id,
                id,
                descriptor,
                rx_queue: mut queue,
                mut replayed,
            } => {
                let message_id = message.id;
                // We are in a replay phase, attempt to handle the message as a
                // `ReplayEvent` from the persistence
                match ReplayEvent::try_from(message) {
                    Ok(replay_event) => match replay_event {
                        // Message replayed
                        ReplayEvent::MessageReplayed(mut msg) => {
                            let replay_id = msg.replay_id.to_uuid();

                            debug!("replaying message with id {replay_id}");

                            if replayed.is_empty() {
                                let _ = self.events_tx.send(PersistenceEvent::ReplayStarted);
                            }

                            if replay_id != id {
                                Err((
                                    State::ReplayPhase {
                                        peers,
                                        command_id,
                                        id,
                                        descriptor,
                                        rx_queue: queue,
                                        replayed,
                                    },
                                    PersistenceError::ConflictingReplay {
                                        replay_id: id,
                                        conflicting_id: replay_id,
                                    },
                                ))
                            } else {
                                replayed.insert(message_id);
                                // In the replay phase, forward a replayed message
                                // directly
                                // Since the message comes from the persistence, it was
                                // persisted, but force it to true to be compatible with
                                // older versions of zebus that did not specify it
                                msg.message.was_persisted = Some(true);
                                let _ = self
                                    .forward_tx
                                    .send(TransportMessage::from_protobuf(msg.message));

                                // Stay in replay phase
                                Ok(State::ReplayPhase {
                                    peers,
                                    command_id,
                                    id,
                                    descriptor,
                                    rx_queue: queue,
                                    replayed,
                                })
                            }
                        }
                        // Replay phase ended
                        ReplayEvent::ReplayPhaseEnded(msg) => {
                            let replay_id = msg.replay_id.to_uuid();
                            if replay_id != id {
                                Err((
                                    State::SafetyPhase {
                                        descriptor,
                                        peers,
                                        id,
                                        replayed,
                                    },
                                    PersistenceError::ConflictingReplay {
                                        replay_id: id,
                                        conflicting_id: replay_id,
                                    },
                                ))
                            } else {
                                info!("switching to safety phase");

                                // Forward queued messages
                                for mut msg in queue.drain(..) {
                                    msg.was_persisted = true;
                                    let _ = self.forward_tx.send(msg);
                                }

                                let _ = self.events_tx.send(PersistenceEvent::SafetyStarted);

                                // Switch to safety phase
                                Ok(State::SafetyPhase {
                                    descriptor,
                                    peers,
                                    id,
                                    replayed,
                                })
                            }
                        }
                        // We should not receive a `SafetyPhaseEnded` event in a replay
                        // phase
                        ReplayEvent::SafetyPhaseEnded(_msg) => Err((
                            State::ReplayPhase {
                                peers,
                                command_id,
                                id,
                                descriptor,
                                rx_queue: queue,
                                replayed,
                            },
                            PersistenceError::InvalidPhase,
                        )),
                    },
                    Err(e) => match e {
                        // Failed to decode incomming message
                        TryFromReplayEventError::Decode(e) => Err((
                            State::ReplayPhase {
                                peers,
                                command_id,
                                id,
                                descriptor,
                                rx_queue: queue,
                                replayed,
                            },
                            PersistenceError::Decode(e),
                        )),

                        // Incomming message is not a `ReplayEvent`
                        TryFromReplayEventError::Other(msg) => {
                            // Check if we received the `MessageExecutionCompleted`
                            // for the `StartMessageReplayCommand`
                            if let Some(message_execution_completed) =
                                msg.decode_as::<MessageExecutionCompleted>()
                            {
                                match message_execution_completed {
                                    Ok(message_execution_completed) => {
                                        let msg_id = MessageId::from_protobuf(
                                            message_execution_completed.command_id,
                                        );
                                        if msg_id.value() == command_id {
                                            let result: CommandResult =
                                                message_execution_completed.into();

                                            // We received a `MessageExecutionCompleted` for the `StartMessageReplayCommand` with
                                            // an error response
                                            if let Err(CommandError::Command { code, message }) =
                                                result
                                            {
                                                error!("failed to start replay: error code ({code}) ({})", message.unwrap_or(String::new()));
                                            }
                                        } else {
                                            // Forward the message
                                            let _ = self.forward_tx.send(msg);
                                        }
                                    }
                                    Err(e) => {
                                        error!("error decoding `MessageExecutionCompleted`: {e}")
                                    }
                                }
                            } else {
                                // When we are in a replay phase, only forward messages that are infrastructure messages
                                let msg_type = msg
                                    .message_type()
                                    .expect("a TransportMessage should always have a message type");

                                if let Some(msg_type_id) =
                                    descriptor.subscriptions.iter().find_map(|s| {
                                        (s.full_name() == msg_type).then_some(s.message_type())
                                    })
                                {
                                    if let Some(true) = msg_type_id.is_infrastructure() {
                                        let _ = self.forward_tx.send(msg);
                                    } else {
                                        // Otherwise, queue it
                                        queue.push(msg);
                                    }
                                }
                            }

                            // Stay in replay phase
                            Ok(State::ReplayPhase {
                                id,
                                command_id,
                                peers,
                                descriptor,
                                rx_queue: queue,
                                replayed,
                            })
                        }
                    },
                }
            }

            // Safety phase
            State::SafetyPhase {
                descriptor,
                peers,
                id,
                mut replayed,
            } => {
                let message_id = message.id;
                // We are in a safety phase, attempt to handle the message as a
                // `ReplayEvent` from the persistence
                match ReplayEvent::try_from(message) {
                    Ok(replay_event) => match replay_event {
                        // Message replayed
                        ReplayEvent::MessageReplayed(mut msg) => {
                            let replay_id = msg.replay_id.to_uuid();

                            debug!("forwarding message with id {replay_id}");

                            if replay_id != id {
                                Err((
                                    State::SafetyPhase {
                                        descriptor,
                                        peers,
                                        id,
                                        replayed,
                                    },
                                    PersistenceError::ConflictingReplay {
                                        replay_id: id,
                                        conflicting_id: replay_id,
                                    },
                                ))
                            } else {
                                // Since the message comes from the persistence, it was
                                // persisted, but force it to true to be compatible with
                                // older versions of zebus that did not specify it
                                msg.message.was_persisted = Some(true);
                                // We received a message to replay from the persistence while we are in the safety
                                // phase, only forward it if we did not already replay it
                                if !replayed.contains(&message_id) {
                                    let _ = self
                                        .forward_tx
                                        .send(TransportMessage::from_protobuf(msg.message));

                                    replayed.insert(message_id);
                                }

                                // Stay in safety phase
                                Ok(State::SafetyPhase {
                                    descriptor,
                                    peers,
                                    id,
                                    replayed,
                                })
                            }
                        }

                        // We should not receive a `ReplayPhaseEnded` while we are in
                        // safety phase
                        ReplayEvent::ReplayPhaseEnded(_msg) => Err((
                            State::SafetyPhase {
                                descriptor,
                                peers,
                                id,
                                replayed,
                            },
                            PersistenceError::InvalidPhase,
                        )),

                        // Safety phase ended
                        ReplayEvent::SafetyPhaseEnded(msg) => {
                            let replay_id = msg.replay_id.to_uuid();

                            if replay_id != id {
                                Err((
                                    State::NormalPhase { peers },
                                    PersistenceError::ConflictingReplay {
                                        replay_id: id,
                                        conflicting_id: replay_id,
                                    },
                                ))
                            } else {
                                info!("switching to normal phase");

                                let _ = self.events_tx.send(PersistenceEvent::Normal);

                                // Switch to normal phase
                                Ok(State::NormalPhase { peers })
                            }
                        }
                    },
                    Err(e) => match e {
                        // Failed to decode incomming message
                        TryFromReplayEventError::Decode(e) => Err((
                            State::SafetyPhase {
                                descriptor,
                                peers,
                                id,
                                replayed,
                            },
                            PersistenceError::Decode(e),
                        )),

                        // Incomming message is not a replay event
                        TryFromReplayEventError::Other(msg) => {
                            // We received a message to replay from the persistence while we are in the safety
                            // phase, only forward it if we did not already replay it
                            if !replayed.contains(&message_id) {
                                let _ = self.forward_tx.send(msg);
                                replayed.insert(message_id);
                            }

                            // Stay in safety phase
                            Ok(State::SafetyPhase {
                                descriptor,
                                peers,
                                id,
                                replayed,
                            })
                        }
                    },
                }
            }
            s => {
                let _ = self.forward_tx.send(message);
                Ok(s)
            }
        }
    }

    async fn persist_message(
        &mut self,
        req: PersistenceRequest,
        state: &mut State,
    ) -> Result<(), PersistenceError> {
        let persistent_peer_ids = req.persistent_peer_ids(self.directory.as_ref());
        let should_persist = !persistent_peer_ids.is_empty();

        let target_peers = req.peers.into_iter().filter(|p| p.is_up);

        match state {
            State::Init {
                ref mut tx_queue, ..
            }
            | State::Registered {
                ref mut tx_queue, ..
            }
            | State::PersistenceReady {
                ref mut tx_queue, ..
            } => {
                // We are still waiting for the persistence service peer
                // The message does not have any persistent peer targets. Send the message right away
                if !should_persist {
                    self.send(target_peers, req.message, SendContext::default())
                        .await?;
                } else {
                    // We need to send the message to the persistence
                    // First send the message right away
                    self.send(target_peers, req.message.clone(), SendContext::default())
                        .await?;

                    // Enqueue a command to persist the message
                    tx_queue.push(PersistenceMessage::Persist {
                        message: req.message,
                        persistent_peer_ids,
                    });
                }
                Ok(())
            }

            State::ReplayPhase { peers, .. }
            | State::SafetyPhase { peers, .. }
            | State::NormalPhase { peers, .. } => {
                let context = if should_persist {
                    let persistence_peer = peers
                        .first()
                        .expect("we should have at least one available persistence peer")
                        .clone();
                    SendContext::Persistent {
                        persistent_peer_ids,
                        persistence_peer,
                    }
                } else {
                    SendContext::Empty
                };

                self.send(target_peers, req.message, context).await
            }
        }
    }

    async fn send(
        &mut self,
        peers: impl IntoIterator<Item = Peer>,
        message: impl Into<TransportMessage>,
        context: SendContext,
    ) -> Result<(), PersistenceError> {
        // TODO(oktal): we could get rid of the double map_err by making the Transport::send function directly
        // return a future that can resolve to an error instead of a Result<Future, Err>
        self.inner
            .send(peers.into_iter(), message.into(), context)
            .map_err(|e| PersistenceError::Transport(e.into()))?
            .await
            .map_err(|e| PersistenceError::Transport(e.into()))
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
    directory: Arc<dyn DirectoryReader>,
    bus_events_rx: S,
    peer: Peer,
    environment: String,
    inner: T,
    forward_tx: broadcast::Sender<TransportMessage>,
    shutdown: CancellationToken,
) -> (
    mpsc::Sender<PersistenceRequest>,
    BoxStream<'static, PersistenceEvent>,
    tokio::task::JoinHandle<Result<(), PersistenceError>>,
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
            directory,
            peer,
            environment,
            inner,
            forward_tx,
            reqs_rx: rx,
            events_tx,
            shutdown,
        };

        //let fut = super::future::StopFuture::spawn(service.run(bus_events_rx));
        let handle = tokio::spawn(service.run(bus_events_rx));
        (tx, events_rx.boxed(), handle)
    } else {
        let service = TransientService {
            inner,
            forward_tx,
            reqs_rx: rx,
            shutdown,
        };

        let events_rx = futures_util::stream::iter([
            PersistenceEvent::ReplayStarted,
            PersistenceEvent::SafetyStarted,
            PersistenceEvent::Normal,
        ]);
        let handle = tokio::spawn(service.run());
        (tx, events_rx.boxed(), handle)
    }
}
