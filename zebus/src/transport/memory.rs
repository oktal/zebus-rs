#![allow(dead_code)]

use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_core::Stream;
use futures_util::pin_mut;
use thiserror::Error;
use tokio::{
    select,
    sync::{broadcast, Notify},
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::BusEvent, core::MessagePayload, directory::DirectoryReader, sync::stream::EventStream,
    Message, MessageDescriptor, MessageTypeId, Peer, PeerId,
};

use super::{SendContext, Transport, TransportMessage};

/// Inner [`MemoryTransport`] state
struct MemoryTransportInner {
    /// Configured peer id
    peer_id: Option<PeerId>,

    /// Configured environment
    environment: Option<String>,

    /// Flag indicating whether the transport has been started
    started: bool,

    /// Sender channel for transport messages
    rcv_tx: Option<broadcast::Sender<TransportMessage>>,

    /// Transmit queue
    /// Messages that are sent through the transport will be stored in this queue along with
    /// the recipient peers
    tx_queue: Vec<(TransportMessage, Vec<Peer>)>,

    /// Waiting transmission queue
    tx_wait_queue: HashMap<String, Arc<Notify>>,

    /// Reception queue
    /// Messages that should be "sent" back as a response to a transport message will be stored
    /// in this queue.
    ///
    /// This queue holds two callbacks:
    /// First callback is a predicate that will be used to determine whether the transport
    /// should respond to a particular message
    /// Second callback will be used to create an instance of a transport message that should
    /// be sent back
    rx_queue: Vec<(
        Box<dyn Fn(&TransportMessage, &Peer) -> bool + Send + 'static>,
        Box<dyn FnOnce(TransportMessage, Peer, String) -> TransportMessage + Send + 'static>,
    )>,
}

/// A [`Transport`] that stores state in memory and has simplified logic for test purposes
#[derive(Clone)]
pub(crate) struct MemoryTransport {
    /// The peer the transport is operating on
    peer: Peer,

    /// Environment
    environment: String,

    /// Shared transport state
    inner: Arc<Mutex<MemoryTransportInner>>,
}

#[derive(Debug, Error)]
pub(crate) enum MemoryTransportError {
    #[error("invalid operation")]
    InvalidOperation,
}

async fn receive<S>(
    stream: S,
    tx: tokio::sync::mpsc::Sender<TransportMessage>,
    cancel: CancellationToken,
) where
    S: Stream<Item = TransportMessage> + Send,
{
    pin_mut!(stream);

    loop {
        select! {
            _ = cancel.cancelled() => break,
            msg = stream.next() => {
                let Some(msg) = msg else {
                    break;
                };

                let _ = tx.send(msg).await;
            }
        }
    }
}

pub(crate) struct MemoryReceiver {
    rx: tokio::sync::mpsc::Receiver<TransportMessage>,
    cancel: CancellationToken,
}

impl MemoryReceiver {
    pub(crate) fn spawn<S>(stream: S) -> Self
    where
        S: Stream<Item = TransportMessage> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let cancel = CancellationToken::new();

        tokio::spawn(receive(stream, tx, cancel.clone()));

        Self { rx, cancel }
    }

    pub(crate) fn shutdown(&self) {
        self.cancel.cancel();
    }

    pub(crate) async fn recv(&mut self) -> Option<TransportMessage> {
        self.rx.recv().await
    }

    pub(crate) fn recv_all(&mut self) -> Vec<TransportMessage> {
        let mut messages = Vec::new();

        while let Ok(msg) = self.rx.try_recv() {
            messages.push(msg);
        }

        messages
    }
}

impl MemoryTransport {
    pub(crate) fn new(peer: Peer) -> Self {
        Self {
            peer,
            environment: "test".to_string(),
            inner: Arc::new(Mutex::new(MemoryTransportInner {
                peer_id: None,
                environment: None,
                started: false,
                rcv_tx: None,
                tx_queue: Vec::new(),
                tx_wait_queue: HashMap::new(),
                rx_queue: Vec::new(),
            })),
        }
    }

    pub(crate) fn is_started(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.started
    }

    /// Queue a message that will be sent back as a response to a transport message
    pub(crate) fn queue_message<M: Message>(
        &self,
        predicate: impl Fn(&TransportMessage, &Peer) -> bool + Send + 'static,
        message_fn: impl Fn(TransportMessage) -> M + Send + Sync + 'static,
    ) -> Option<()> {
        let message_fn = Box::new(message_fn);
        let create_fn = Box::new(move |transport_message, sender, environment| {
            let message = message_fn(transport_message);
            let (_id, transport) = TransportMessage::create(&sender, environment, &message);
            transport
        });

        let mut inner = self.inner.lock().unwrap();
        inner.rx_queue.push((Box::new(predicate), create_fn));
        Some(())
    }

    pub(crate) fn message_received<M: crate::Message + prost::Message>(
        &self,
        message: M,
        sender: &Peer,
        environment: String,
    ) -> Option<uuid::Uuid> {
        let (id, transport) = TransportMessage::create(sender, environment, &message);
        self.transport_message_received(transport)?;
        Some(id)
    }

    pub(crate) fn transport_message_received(&self, message: TransportMessage) -> Option<()> {
        let inner = self.inner.lock().unwrap();
        let rcv_tx = inner.rcv_tx.as_ref()?;
        rcv_tx.send(message).ok()?;
        Some(())
    }

    /// Wait for [`count`] messages of type `M` to be sent through the transport
    pub(crate) async fn wait_for<M: MessageDescriptor + prost::Message + Default + 'static>(
        &self,
        count: usize,
    ) -> Vec<(M, Vec<Peer>)> {
        loop {
            // First attempt to retrieve messages from the tx_queue
            let tx_messages = self.get::<M>();

            // If there is enough messages already in the tx_queue, return right away
            if tx_messages.len() >= count {
                return tx_messages;
            }

            let message_type = MessageTypeId::of::<M>();

            // We need to wait for more messages to be sent through the transport
            let notify = {
                let mut inner = self.inner.lock().unwrap();

                inner
                    .tx_wait_queue
                    .entry(message_type.into_name())
                    .or_insert(Arc::new(Notify::new()))
                    .clone()
            };

            notify.notified().await;
        }
    }

    /// Get the list of messages that have been sent through the transport
    pub(crate) fn get<M: MessageDescriptor + prost::Message + Default>(
        &self,
    ) -> Vec<(M, Vec<Peer>)> {
        let inner = self.inner.lock().unwrap();
        inner
            .tx_queue
            .iter()
            .filter_map(|(msg, peers)| {
                let message = msg.decode_as::<M>()?.ok()?;
                Some((message, peers.clone()))
            })
            .collect()
    }

    /// Get the configured peer id
    pub(crate) fn get_peer_id(&self) -> Option<PeerId> {
        let inner = self.inner.lock().unwrap();
        inner.peer_id.clone()
    }

    /// Get the configured environment
    pub(crate) fn get_environment(&self) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        inner.environment.clone()
    }

    pub(crate) fn spawn(&self) -> Result<MemoryReceiver, MemoryTransportError> {
        self.subscribe().map(MemoryReceiver::spawn)
    }
}

impl Transport for MemoryTransport {
    type Err = MemoryTransportError;
    type MessageStream = crate::sync::stream::BroadcastStream<TransportMessage>;

    type StartCompletionFuture = futures_util::future::Ready<std::result::Result<(), Self::Err>>;
    type StopCompletionFuture = futures_util::future::Ready<std::result::Result<(), Self::Err>>;
    type SendFuture = futures_util::future::Ready<std::result::Result<(), Self::Err>>;

    fn configure(
        &mut self,
        peer_id: PeerId,
        environment: String,
        _directory: Arc<dyn DirectoryReader>,
        _event_rx: EventStream<BusEvent>,
    ) -> Result<(), MemoryTransportError> {
        let mut inner = self.inner.lock().unwrap();
        inner.peer_id = Some(peer_id);
        inner.environment = Some(environment);
        Ok(())
    }

    fn subscribe(&self) -> Result<Self::MessageStream, Self::Err> {
        let inner = self.inner.lock().unwrap();
        match inner.rcv_tx.as_ref() {
            Some(rcv_tx) => Ok(rcv_tx.subscribe().into()),
            None => Err(MemoryTransportError::InvalidOperation),
        }
    }

    fn start(&mut self) -> Result<Self::StartCompletionFuture, MemoryTransportError> {
        let mut inner = self.inner.lock().unwrap();
        inner.started = true;
        let (rcv_tx, _) = broadcast::channel(128);
        inner.rcv_tx = Some(rcv_tx);
        Ok(futures_util::future::ready(Ok(())))
    }

    fn stop(&mut self) -> Result<Self::StopCompletionFuture, MemoryTransportError> {
        Ok(futures_util::future::ready(Ok(())))
    }

    fn peer_id(&self) -> Result<&PeerId, Self::Err> {
        Ok(&self.peer.id)
    }

    fn environment(&self) -> Result<Cow<'_, str>, Self::Err> {
        Ok(Cow::Borrowed(self.environment.as_str()))
    }

    fn inbound_endpoint(&self) -> Result<Cow<'_, str>, Self::Err> {
        Ok(Cow::Owned("tcp://localhost:5050".to_string()))
    }

    fn send(
        &mut self,
        peers: impl Iterator<Item = Peer>,
        message: TransportMessage,
        _context: SendContext,
    ) -> std::result::Result<Self::SendFuture, Self::Err> {
        let peers: Vec<_> = peers.collect();

        let mut inner = self.inner.lock().unwrap();
        let environment = inner.environment.clone().unwrap();

        // TODO(oktal): drain_filter
        for peer in &peers {
            let mut i = 0;
            while i < inner.rx_queue.len() {
                let entry = &inner.rx_queue[i];
                if (entry.0)(&message, peer) {
                    let entry = inner.rx_queue.remove(i);
                    let response =
                        (entry.1)(message.clone(), self.peer.clone(), environment.clone());
                    let tx = inner.rcv_tx.as_ref().unwrap();
                    tx.send(response).unwrap();
                } else {
                    i += 1;
                }
            }
        }

        let msg_type = message
            .message_type()
            .expect("a TransportMessage should always have a message type")
            .to_string();
        inner.tx_queue.push((message, peers));

        // Notify any waiter that some messages have been sent
        if let Some(notify) = inner.tx_wait_queue.get(&msg_type) {
            notify.notify_waiters();
        }

        Ok(futures_util::future::ready(Ok(())))
    }
}
