use std::{borrow::Cow, sync::Arc};

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::BusEvent,
    directory::DirectoryReader,
    sync::stream::EventStream,
    transport::{self, future::SendFuture, SendContext, Transport, TransportMessage},
    BusConfiguration, Peer, PeerId,
};

use super::{PersistenceError, PersistenceRequest};

enum Inner<T> {
    Init {
        /// Configuration of the bus
        configuration: BusConfiguration,

        /// Inner transport
        inner: T,
    },

    Configured {
        /// Configuration of the bus
        configuration: BusConfiguration,

        /// [`PeerId`] that has ben configured
        peer_id: PeerId,

        /// Environment that has been configured
        environment: String,

        /// Inner transport
        inner: T,

        /// Directory reader
        directory: Arc<dyn DirectoryReader>,

        /// Bus event reception stream
        event_rx: EventStream<BusEvent>,
    },

    Started {
        /// Configuration of the bus
        _configuration: BusConfiguration,

        /// Current [`Peer`]
        peer: Peer,

        /// Environment that has been configured
        environment: String,

        /// Channel to receive [`TransportMessage`] messages from
        /// Use to subscribe
        messages_tx: broadcast::Sender<TransportMessage>,

        /// Channel to send requests to persist messages
        requests_tx: tokio::sync::mpsc::Sender<PersistenceRequest>,

        /// Token to cancel task
        shutdown: CancellationToken,

        /// Future that resolves when the task has been joined
        stop: super::future::StopFuture,
    },
}

pub struct PersistentTransport<T> {
    inner: Option<Inner<T>>,
}

impl<T> PersistentTransport<T>
where
    T: Transport,
{
    pub(crate) fn new(configuration: BusConfiguration, inner: T) -> Self {
        Self {
            inner: Some(Inner::Init {
                configuration,
                inner,
            }),
        }
    }
}

impl<T> Transport for PersistentTransport<T>
where
    T: Transport,
{
    type Err = PersistenceError;
    type MessageStream = super::MessageStream;

    type StartCompletionFuture = super::future::StartFuture;
    type StopCompletionFuture = super::future::StopFuture;
    type SendFuture = transport::future::SendFuture<PersistenceRequest, Self::Err>;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        directory: Arc<dyn DirectoryReader>,
        event: EventStream<BusEvent>,
    ) -> Result<(), Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init {
                configuration,
                mut inner,
            }) => {
                // Configure the inner transport
                inner
                    .configure(
                        peer_id.clone(),
                        environment.clone(),
                        Arc::clone(&directory),
                        event.clone(),
                    )
                    .map_err(Into::into)?;

                // Transition to Configured state
                (
                    Some(Inner::Configured {
                        configuration,
                        peer_id,
                        environment,
                        directory,
                        inner,
                        event_rx: event,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(PersistenceError::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref messages_tx, ..
            }) => Ok(messages_tx.subscribe().into()),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn start(&mut self) -> Result<Self::StartCompletionFuture, Self::Err> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Configured {
                configuration,
                peer_id,
                environment,
                directory,
                mut inner,
                event_rx,
            }) => {
                // Start the underlying transport
                inner.start().map_err(Into::into)?;

                // Retrieve bound endpoint
                let inbound_endpoint = inner.inbound_endpoint().map_err(Into::into)?;

                let peer = Peer::new(peer_id, inbound_endpoint);

                // Create broadcast channel for transport message reception
                let (messages_tx, _messages_rx) = broadcast::channel(128);

                let shutdown = CancellationToken::new();

                // Spawn a new task for the persistence service
                let (requests_tx, events_rx, stop) = super::service::spawn(
                    &configuration,
                    directory,
                    event_rx.stream(),
                    peer.clone(),
                    environment.clone(),
                    inner,
                    messages_tx.clone(),
                    shutdown.clone(),
                );

                // Spawn a future that will resolve when the start sequence completes
                let start = super::future::StartFuture::spawn(events_rx, &configuration);

                // Transition to Started state
                (
                    Some(Inner::Started {
                        _configuration: configuration,
                        peer,
                        environment,
                        messages_tx,
                        requests_tx,
                        shutdown,
                        stop,
                    }),
                    Ok(start),
                )
            }
            x => (x, Err(PersistenceError::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    fn stop(&mut self) -> Result<Self::StopCompletionFuture, Self::Err> {
        match self.inner.take() {
            Some(Inner::Started { shutdown, stop, .. }) => {
                // Cancel the task
                shutdown.cancel();

                // TODO(oktal): we should go back to the `Configured` state but we need to retrieve
                // the inner transport layer that was moved to an inner task

                Ok(stop)
            }
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured { peer_id, .. }) => Ok(peer_id),
            Some(Inner::Started { peer, .. }) => Ok(&peer.id),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn environment(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Configured {
                ref environment, ..
            })
            | Some(Inner::Started {
                ref environment, ..
            }) => Ok(Cow::Borrowed(environment.as_str())),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { peer, .. }) => Ok(Cow::Borrowed(&peer.endpoint)),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        _context: SendContext,
    ) -> Result<Self::SendFuture, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { requests_tx, .. }) => Ok(SendFuture::new(
                requests_tx.clone(),
                peers,
                message,
                SendContext::default(),
            )),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use futures_util::pin_mut;
    use tokio_stream::StreamExt;

    use crate::{
        bus::BusEvent,
        core::MessagePayload,
        directory::{memory::MemoryDirectory, Directory, PeerDescriptor},
        persistence::{
            command::{PersistMessageCommand, StartMessageReplayCommand},
            event::{ReplayPhaseEnded, SafetyPhaseEnded},
            PersistenceError,
        },
        proto::IntoProtobuf,
        sync::stream::EventStream,
        transport::{
            memory::{MemoryReceiver, MemoryTransport},
            Transport, TransportExt, TransportMessage,
        },
        BusConfiguration, Command, Message, MessageExt, Peer, PeerId, Subscription,
    };

    use super::PersistentTransport;

    #[derive(prost::Message, Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test")]
    struct TestCommand {}

    #[derive(prost::Message, Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test")]
    struct TestReplayCommand {
        #[prost(fixed32, required, tag = 1)]
        id: u32,
    }

    struct Fixture {
        peer: Peer,
        persistence_peer: PeerDescriptor,
        environment: String,

        events: EventStream<BusEvent>,

        directory: Arc<MemoryDirectory>,
        inner: MemoryTransport,

        transport: PersistentTransport<MemoryTransport>,
    }

    impl Fixture {
        fn new(configuration: BusConfiguration) -> Self {
            let peer = Peer::test();
            let persistence_peer = PeerDescriptor {
                peer: Peer {
                    id: PeerId::new("PersistenceService.0"),
                    endpoint: "tcp://localhost:7865".to_string(),
                    is_up: true,
                    is_responding: true,
                },
                subscriptions: vec![Subscription::any::<PersistMessageCommand>()],
                is_persistent: true,
                timestamp_utc: Some(Utc::now()),
                has_debugger_attached: Some(false),
            };

            let environment = "test".to_string();
            let events = EventStream::new(16);
            let inner = MemoryTransport::new(peer.clone());

            Self {
                peer,
                persistence_peer,
                environment,
                directory: MemoryDirectory::new(),
                inner: inner.clone(),
                events,
                transport: inner.persistent(configuration),
            }
        }

        fn configuration() -> BusConfiguration {
            let mut configuration = BusConfiguration::default();
            configuration.is_persistent = true;
            configuration
        }

        fn new_default() -> Self {
            Self::new(Self::configuration())
        }

        fn configure(&mut self) -> Result<(), PersistenceError> {
            self.transport.configure(
                self.peer.id.clone(),
                self.environment.clone(),
                self.directory.reader(),
                self.events.clone(),
            )
        }

        fn descriptor(
            &self,
            subscriptions: impl IntoIterator<Item = Subscription>,
        ) -> PeerDescriptor {
            PeerDescriptor {
                peer: self.peer.clone(),
                subscriptions: subscriptions.into_iter().collect(),
                is_persistent: true,
                timestamp_utc: None,
                has_debugger_attached: Some(false),
            }
        }

        /// Simulate a registration to the directory by sending the according `BusEvent`
        fn register(&self, subscriptions: impl IntoIterator<Item = Subscription>) {
            self.events
                .send(BusEvent::Registering(self.descriptor(subscriptions)))
                .expect("send should not fail");

            self.events
                .send(BusEvent::Registered(vec![self.persistence_peer.clone()]))
                .expect("send should not fail");
        }

        /// Wait for the `StartMessageReplayCommand` to be sent the persistent transport layer
        /// for a [`timeout`] specific amount of time
        ///
        /// Returns `Some` with the replay id if the command was received before the timeout
        /// or `None` otherwise
        async fn wait_for_start(&mut self, timeout: Duration) -> Option<uuid::Uuid> {
            let start =
                tokio::time::timeout(timeout, self.inner.wait_for::<StartMessageReplayCommand>(1))
                    .await;

            match start {
                Ok(start) => {
                    let start = start
                        .get(0)
                        .expect("we should have received a start replay message");

                    let replay_id = start.0.replay_id.to_uuid();
                    Some(replay_id)
                }
                Err(_) => None,
            }
        }

        /// Replay a given message `msg` from a sending `peer`
        fn replay<M>(
            &mut self,
            replay_id: uuid::Uuid,
            peer: Peer,
            msg: M,
            message_fn: impl FnOnce(&mut TransportMessage),
        ) where
            M: Message,
        {
            let (_, message_replayed) = msg.as_replayed(replay_id, &peer, self.environment.clone());

            let (_, mut message_replayed) =
                message_replayed.as_transport(&peer, self.environment.clone());

            message_fn(&mut message_replayed);

            self.inner
                .transport_message_received(message_replayed)
                .expect("message received should not fail");
        }

        fn spawn(&self) -> Result<MemoryReceiver, PersistenceError> {
            self.transport.subscribe().map(MemoryReceiver::spawn)
        }
    }

    #[tokio::test]
    async fn start_should_start_inner_transport() {
        // Setup
        let mut fixture = Fixture::new_default();

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        fixture.transport.start().expect("start should not fail");

        // Make sure that inner transport has been started
        assert!(fixture.inner.is_started());
    }

    #[tokio::test]
    async fn start_should_fail_with_timeout_when_failing_to_reach_persistence() {
        // Setup
        let mut config = Fixture::configuration();
        config.start_replay_timeout = Duration::from_millis(100);
        let mut fixture = Fixture::new(config);

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        let fut = fixture.transport.start().expect("start should not fail");

        // Make sure that start fails with proper error
        assert!(matches!(fut.await, Err(PersistenceError::Unreachable(_))));
    }

    #[tokio::test]
    async fn only_forward_replayed_messages_during_replay_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Make sure a `StartMessageReplayCommand` has been sent
        // TODO(oktal): timeout
        let start = fixture.inner.wait_for::<StartMessageReplayCommand>(1).await;
        let start = start
            .get(0)
            .expect("we should have received a start replay message");

        let replay_id = start.0.replay_id.to_uuid();

        // Subscribe to persistent transport messages
        let rx = fixture
            .transport
            .subscribe()
            .expect("subscribe should not fail")
            .timeout(Duration::from_millis(100));

        pin_mut!(rx);

        let (message_replayed_id, message_replayed) =
            TestCommand {}.as_replayed(replay_id, &Peer::test(), fixture.environment.clone());

        // Send a normal command
        fixture
            .inner
            .message_received(
                TestCommand {},
                &fixture.persistence_peer.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Send a command to replay
        fixture
            .inner
            .message_received(
                message_replayed,
                &fixture.persistence_peer.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Make sure we only forwarded the command to replay
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.id.value(), message_replayed_id);
        assert_eq!(cmd.decode_as::<TestCommand>(), Some(Ok(TestCommand {})));

        // Make sure that we did not forward any more message
        assert!(matches!(rx.try_next().await, Err(_)));
    }

    #[tokio::test]
    async fn start_should_complete_after_first_message_from_persistence() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        let start = fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Wait for `StartMessageReplayCommand` to retrieve the replay id
        let start_cmd = fixture.inner.wait_for::<StartMessageReplayCommand>(1).await;
        let start_cmd = start_cmd
            .get(0)
            .expect("we should have received a start replay message");

        let replay_id = start_cmd.0.replay_id;

        // Send ReplayPhaseEnded
        fixture
            .inner
            .message_received(
                ReplayPhaseEnded { replay_id },
                &fixture.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Wait for the start completion future with a timeout
        let res = tokio::time::timeout(Duration::from_millis(100), start).await;

        // Make sure the start operation finished without an error
        // First Ok(()) means the future resolved with no timeout
        // Second Ok(()) means the start operation finished with no error
        assert!(matches!(res, Ok(Ok(()))));
    }

    #[tokio::test]
    async fn replay_messages_during_replay_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Subscribe to persistent transport messages
        let mut rx = fixture
            .spawn()
            .expect("subscribe to transport messages received should not fail");

        // Wait for replay to start and retrieve the replay id
        let replay_id = fixture
            .wait_for_start(Duration::from_millis(100))
            .await
            .expect("replay should have started");

        // Replay some messages
        let to_replay = (0..100)
            .map(|i| TestReplayCommand { id: i as u32 })
            .collect::<Vec<_>>();

        let replay_peer = Peer::test();

        for message in &to_replay {
            fixture.replay(replay_id, replay_peer.clone(), message.clone(), |_| {});
        }

        // Make sure messages were replayed
        tokio::time::sleep(Duration::from_millis(100)).await;
        let replayed = rx.recv_all();

        assert_eq!(replayed.len(), to_replay.len());

        for (to_replay, replayed) in to_replay.into_iter().zip(replayed) {
            let replayed = replayed.decode_as::<TestReplayCommand>();
            assert_eq!(replayed.unwrap(), Ok(to_replay));
        }
    }

    #[tokio::test]
    async fn set_was_persisted_for_replayed_messages() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Subscribe to persistent transport messages
        let rx = fixture
            .transport
            .subscribe()
            .expect("subscribe should not fail")
            .timeout(Duration::from_millis(100));

        pin_mut!(rx);

        // Wait for replay to start and retrieve the replay id
        let replay_id = fixture
            .wait_for_start(Duration::from_millis(100))
            .await
            .expect("replay should have started");

        // Replay message
        fixture.replay(replay_id, Peer::test(), TestCommand {}, |msg| {
            msg.was_persisted = false
        });

        // Make sure was_persisted was set to true
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.was_persisted, true);
    }

    #[tokio::test]
    async fn set_was_persisted_for_messages_replayed_afer_replay_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([Subscription::any::<TestCommand>()]);

        // Subscribe to persistent transport messages
        let rx = fixture
            .transport
            .subscribe()
            .expect("subscribe should not fail")
            .timeout(Duration::from_millis(100));

        pin_mut!(rx);

        // Wait for replay to start and retrieve the replay id
        let replay_id = fixture
            .wait_for_start(Duration::from_millis(100))
            .await
            .expect("replay should have started");

        // Receive normal message
        let (_, mut message) =
            TestCommand {}.as_transport(&Peer::test(), fixture.environment.clone());
        message.was_persisted = false;
        fixture
            .inner
            .transport_message_received(message)
            .expect("message received should not fail");

        // Send ReplayPhaseEnded
        fixture
            .inner
            .message_received(
                ReplayPhaseEnded {
                    replay_id: replay_id.into_protobuf(),
                },
                &fixture.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Make sure was_persisted was set to true
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.was_persisted, true);
    }

    #[tokio::test]
    async fn set_was_persisted_for_replayed_messages_during_safety_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Subscribe to persistent transport messages
        let rx = fixture
            .transport
            .subscribe()
            .expect("subscribe should not fail")
            .timeout(Duration::from_millis(100));

        pin_mut!(rx);

        // Wait for replay to start and retrieve the replay id
        let replay_id = fixture
            .wait_for_start(Duration::from_millis(100))
            .await
            .expect("replay should have started");

        // Send ReplayPhaseEnded
        fixture
            .inner
            .message_received(
                ReplayPhaseEnded {
                    replay_id: replay_id.into_protobuf(),
                },
                &fixture.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Replay message
        fixture.replay(replay_id, Peer::test(), TestCommand {}, |msg| {
            msg.was_persisted = false
        });

        // Send SafetyPhaseEnded
        fixture
            .inner
            .message_received(
                SafetyPhaseEnded {
                    replay_id: replay_id.into_protobuf(),
                },
                &fixture.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Make sure was_persisted was set to true
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.was_persisted, true);
    }

    #[tokio::test]
    async fn deduplicate_messages_during_safety_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        fixture.configure().expect("configure should not fail");
        fixture.transport.start().expect("start should not fail");
        fixture.register([]);

        // Subscribe to persistent transport messages
        let mut receiver = fixture
            .spawn()
            .expect("spawning message receiver should not fail");

        // Wait for replay to start and retrieve the replay id
        let replay_id = fixture
            .wait_for_start(Duration::from_millis(100))
            .await
            .expect("replay should have started");

        // Send ReplayPhaseEnded
        fixture
            .inner
            .message_received(
                ReplayPhaseEnded {
                    replay_id: replay_id.into_protobuf(),
                },
                &fixture.peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        let replay_command = TestReplayCommand { id: 0xF00D };

        // Receive message
        let msg_id = fixture
            .inner
            .message_received(
                replay_command.clone(),
                &Peer::test(),
                fixture.environment.clone(),
            )
            .unwrap();

        // Replay duplicated message
        fixture.replay(replay_id, Peer::test(), replay_command, |msg| {
            msg.id = msg_id.into()
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received = receiver.recv_all();

        // Make sure we did not receive a duplicated message through the persistence
        assert_eq!(received.len(), 1);
    }
}
