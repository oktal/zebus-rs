use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use chrono::Utc;
use tokio::time::Timeout;

use super::{PeerDescriptor, RegisterPeerCommand, RegisterPeerResponse};
use crate::{
    transport::{self, MessageExecutionCompleted, SendContext, Transport, TransportMessage},
    Peer,
};

/// Provides a [`std::future::Future`] of a registration request to a directory
/// Safety note:
/// Even though there is no direct `unsafe` call, this type, and especially the
/// [`std::future::Future`] implementation of our type is not safe regarding futures.
///
/// The unsafety-ness of the type is due to the fact that we are using a transport
/// [`Receiver`] to receive messsages from the [`Transport`] layer, which is a tokio mpsc
/// receiver. However, as implied by their name, receivers are "single consumer", which means
/// that we can not clone them. The lack of `Clone` means that we need to move the `Receiver`
/// inside our future, but we need to give it back to the original caller of the `register`
/// function to be able to keep receiving messages.
///
/// This is where the `poll` implementation of the [`std::future::Future`] is a little bit tricky
/// as we need to move back the `Receiver` when resolving the future, but we are behind a
/// [`std::pin::Pin`]. To safely move the [`Receiver`] back, we then wrap it inside an `Option`
/// that we `take()` every-time we attempt to `poll` the tokio receiver. When the corresponding
/// [`RegisterPeerResponse`] has been successfully received, we then resolve the future (by
/// returning `Poll::Pending` and *only at that moment*, move the receiver back to the output
/// of the future.
pub(crate) struct RegistrationFuture {
    /// The [`RegisterPeerCommand`] original message id
    message_id: uuid::Uuid,

    /// Receiver that we can poll to check whether we received a message from the [`Transport`]
    /// layer
    receiver: Option<transport::Receiver>,
}

impl RegistrationFuture {
    fn register<T: Transport>(
        transport: &mut T,
        receiver: transport::Receiver,
        self_peer: Peer,
        environment: String,
        directory_endpoint: Peer,
    ) -> Result<Self, T::Err> {
        let utc_now = Utc::now();

        let descriptor = PeerDescriptor {
            peer: self_peer.clone(),
            subscriptions: vec![],
            is_persistent: false,
            timestamp_utc: Some(utc_now.into()),
            has_debugger_attached: Some(false),
        };
        let register_command = RegisterPeerCommand { peer: descriptor };
        let (message_id, message) =
            TransportMessage::create(&self_peer, environment, &register_command);

        let registration = RegistrationFuture {
            message_id,
            receiver: Some(receiver),
        };

        transport.send(
            std::iter::once(directory_endpoint),
            message.clone(),
            SendContext::default(),
        )?;
        Ok(registration)
    }

    pub fn with_timeout(self, timeout: Duration) -> Timeout<Self> {
        tokio::time::timeout(timeout, self)
    }
}

impl Future for RegistrationFuture {
    type Output = (
        Result<RegisterPeerResponse, prost::DecodeError>,
        transport::Receiver,
    );

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (receiver, res) = match self.receiver.take() {
            Some(mut receiver) => {
                match receiver.poll_recv(cx) {
                    Poll::Ready(Some(transport_message)) => {
                        if let Some(message) =
                            transport_message.decode_as::<MessageExecutionCompleted>()
                        {
                            // TODO(oktal): Correctly bubble up the error to the future
                            let message_execution_completed = message.unwrap();
                            // TODO(oktal): check that the `source_command_id` is `message_id` and error
                            // otherwise
                            if let Some(response) =
                                message_execution_completed.decode_as::<RegisterPeerResponse>()
                            {
                                (None, Poll::Ready((response, receiver)))
                            } else {
                                // TODO(oktal): accumulate messages while waiting for RegisterPeerResponse
                                (Some(receiver), Poll::Pending)
                            }
                        } else {
                            (Some(receiver), Poll::Pending)
                        }
                    }
                    _ => (Some(receiver), Poll::Pending),
                }
            }
            _ => panic!("attempted to poll already resolved future"),
        };

        self.receiver = receiver;
        res
    }
}

pub(crate) fn register<T: Transport>(
    transport: &mut T,
    receiver: transport::Receiver,
    self_peer: Peer,
    environment: String,
    directory_endpoint: Peer,
) -> Result<RegistrationFuture, T::Err> {
    RegistrationFuture::register(
        transport,
        receiver,
        self_peer,
        environment,
        directory_endpoint,
    )
}
