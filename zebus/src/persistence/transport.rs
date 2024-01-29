use std::{borrow::Cow, sync::Arc};

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::BusEvent,
    directory::DirectoryReader,
    sync::stream::EventStream,
    transport::{self, future::SendFuture, Transport, TransportMessage},
    BusConfiguration, PeerId,
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

        /// Bus event reception stream
        event_rx: EventStream<BusEvent>,
    },

    Started {
        /// Configuration of the bus
        _configuration: BusConfiguration,

        /// [`PeerId`] that has ben configured
        peer_id: PeerId,

        /// Environment that has been configured
        environment: String,

        /// The endpoint that has been bound by the inner transport
        inbound_endpoint: String,

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
                mut inner,
                event_rx,
            }) => {
                // Start the underlying transport
                inner.start().map_err(Into::into)?;

                // Retrieve bound endpoint
                let inbound_endpoint = inner.inbound_endpoint().map_err(Into::into)?.into_owned();

                // Create broadcast channel for transport message reception
                let (messages_tx, _messages_rx) = broadcast::channel(128);

                let shutdown = CancellationToken::new();

                let (requests_tx, events_rx, stop) = super::service::spawn(
                    &configuration,
                    event_rx.stream(),
                    inner,
                    messages_tx.clone(),
                    shutdown.clone(),
                );

                let start = super::future::StartFuture::spawn(events_rx, &configuration);

                // Transition to Started state
                (
                    Some(Inner::Started {
                        _configuration: configuration,
                        peer_id,
                        environment,
                        inbound_endpoint,
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
            Some(Inner::Configured { ref peer_id, .. })
            | Some(Inner::Started { ref peer_id, .. }) => Ok(peer_id),
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

    fn inbound_endpoint(&self) -> Result<std::borrow::Cow<'_, str>, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started {
                ref inbound_endpoint,
                ..
            }) => Ok(Cow::Borrowed(inbound_endpoint.as_str())),
            _ => Err(PersistenceError::InvalidOperation),
        }
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = crate::Peer>,
        message: TransportMessage,
        _context: crate::transport::SendContext,
    ) -> Result<Self::SendFuture, Self::Err> {
        match self.inner.as_ref() {
            Some(Inner::Started { requests_tx, .. }) => {
                Ok(SendFuture::new(requests_tx.clone(), peers, message))
            }
            _ => Err(PersistenceError::InvalidOperation),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures_util::pin_mut;
    use tokio_stream::StreamExt;

    use crate::{
        bus::BusEvent,
        core::MessagePayload,
        directory::{memory::MemoryDirectory, Directory, PeerDescriptor},
        persistence::{
            command::StartMessageReplayCommand,
            event::{ReplayPhaseEnded, SafetyPhaseEnded},
            PersistenceError,
        },
        proto::IntoProtobuf,
        sync::stream::EventStream,
        transport::{memory::MemoryTransport, Transport, TransportExt},
        BusConfiguration, Command, MessageExt, Peer, PeerId, Subscription,
    };

    use super::PersistentTransport;

    #[derive(prost::Message, Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Test")]
    struct TestCommand {}

    struct Fixture {
        peer: Peer,
        persistence_peer: Peer,
        environment: String,

        events: EventStream<BusEvent>,

        directory: Arc<MemoryDirectory>,
        inner: MemoryTransport,

        transport: PersistentTransport<MemoryTransport>,
    }

    impl Fixture {
        fn new(configuration: BusConfiguration) -> Self {
            let peer = Peer::test();
            let persistence_peer = Peer {
                id: PeerId::new("Abc.Zebus.PersistenceService.0"),
                endpoint: "tcp://localhost:7865".to_string(),
                is_up: true,
                is_responding: true,
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

        // Wait for the `StartMessageReplayCommand` to be sent the persistent transport layer
        // for a [`timeout`] specific amount of time
        //
        // Returns `Some` with the replay id if the command was received before the timeout
        // or `None` otherwise
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

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        fixture.transport.start().expect("start should not fail");

        // Start replay
        fixture
            .events
            .send(BusEvent::Registered(vec![fixture.persistence_peer.clone()]))
            .expect("send should not fail");

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
                &fixture.persistence_peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Send a command to replay
        fixture
            .inner
            .message_received(
                message_replayed,
                &fixture.persistence_peer,
                fixture.environment.clone(),
            )
            .expect("message received should not fail");

        // Make sure we only forwarded the command to replay
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.id.value.to_uuid(), message_replayed_id);
        assert_eq!(cmd.decode_as::<TestCommand>(), Some(Ok(TestCommand {})));

        // Make sure that we did not forward any more message
        assert!(matches!(rx.try_next().await, Err(_)));
    }

    #[tokio::test]
    async fn start_should_complete_after_first_message_from_persistence() {
        // Setup
        let mut fixture = Fixture::new_default();

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        let start = fixture.transport.start().expect("start should not fail");

        // Start replay
        fixture
            .events
            .send(BusEvent::Registered(vec![fixture.persistence_peer.clone()]))
            .expect("send should not fail");

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
    async fn set_was_persisted_for_replayed_messages() {
        // Setup
        let mut fixture = Fixture::new_default();

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        fixture.transport.start().expect("start should not fail");

        // Start replay
        fixture
            .events
            .send(BusEvent::Registered(vec![fixture.persistence_peer.clone()]))
            .expect("send should not fail");

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

        // Create message replayed
        let (_, message_replayed) =
            TestCommand {}.as_replayed(replay_id, &Peer::test(), fixture.environment.clone());

        let (_, mut message_replayed) =
            message_replayed.as_transport(&Peer::test(), fixture.environment.clone());

        message_replayed.was_persisted = Some(false);

        // Replay message
        fixture
            .inner
            .transport_message_received(message_replayed)
            .expect("message received should not fail");

        // Make sure was_persisted was set to true
        let cmd = rx
            .next()
            .await
            .expect("stream should not be finished")
            .expect("we should have forwarded a message");

        assert_eq!(cmd.was_persisted, Some(true));
    }

    #[tokio::test]
    async fn set_was_persisted_for_messages_replayed_afer_replay_phase() {
        // Setup
        let mut fixture = Fixture::new_default();
        let descriptor = fixture.descriptor([Subscription::any::<TestCommand>()]);

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        fixture.transport.start().expect("start should not fail");

        // Start replay
        fixture
            .events
            .send(BusEvent::Registering(descriptor))
            .expect("send should not fail");

        fixture
            .events
            .send(BusEvent::Registered(vec![fixture.persistence_peer.clone()]))
            .expect("send should not fail");

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
        message.was_persisted = Some(false);
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

        assert_eq!(cmd.was_persisted, Some(true));
    }

    #[tokio::test]
    async fn set_was_persisted_for_replayed_messages_during_safety_phase() {
        // Setup
        let mut fixture = Fixture::new_default();

        // Configure transport
        fixture.configure().expect("configure should not fail");

        // Start transport
        fixture.transport.start().expect("start should not fail");

        // Start replay
        fixture
            .events
            .send(BusEvent::Registered(vec![fixture.persistence_peer.clone()]))
            .expect("send should not fail");

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

        // Create message replayed
        let (_, message_replayed) =
            TestCommand {}.as_replayed(replay_id, &Peer::test(), fixture.environment.clone());

        let (_, mut message_replayed) =
            message_replayed.as_transport(&Peer::test(), fixture.environment.clone());

        message_replayed.was_persisted = Some(false);

        // Replay message
        fixture
            .inner
            .transport_message_received(message_replayed)
            .expect("message received should not fail");

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

        assert_eq!(cmd.was_persisted, Some(true));
    }
}
