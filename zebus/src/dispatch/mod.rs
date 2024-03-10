mod context;
mod dispatcher;
mod future;
mod invoker;
mod queue;
pub mod router;

use std::convert::Infallible;
use std::{fmt::Display, sync::Arc};

use crate::bus::{CommandError, CommandResult};
use crate::core::{response::HANDLER_ERROR_CODE, Response};
use crate::core::{MessagePayload, RawMessage};
use crate::lotus::MessageProcessingFailed;
use crate::proto::IntoProtobuf;
use crate::{
    transport::{MessageExecutionCompleted, TransportMessage},
    BoxError, MessageKind, MessageTypeDescriptor, Peer,
};
use crate::{Bus, Message, MessageDescriptor, MessageId};
pub(crate) use context::DispatchContext;
pub(crate) use dispatcher::{Error, MessageDispatcher};
use futures_core::future::BoxFuture;
pub(crate) use invoker::{HandlerResponse, MessageInvokerDescriptor};
pub use invoker::{InvokeRequest, InvokerService};
pub use router::{IntoHandler, RouteHandler, RouteHandlerDescriptor, Router};
use tower_service::Service;

/// A message to dispatch
#[derive(Debug, Clone)]
pub(crate) enum DispatchMessage {
    /// Dispatch a [`TransportMessage`] from a remote peer
    Remote(TransportMessage),

    /// Dispatch a local [`Message`]
    Local {
        /// Current [`Peer`]
        self_peer: Peer,

        /// Current environment
        environment: String,

        /// [`Message`] to dispatch
        message: Arc<dyn Message>,
    },
}

impl MessagePayload for DispatchMessage {
    fn message_type(&self) -> Option<&str> {
        match self {
            DispatchMessage::Remote(message) => message.message_type(),
            DispatchMessage::Local { message, .. } => Some(message.name()),
        }
    }

    fn content(&self) -> Option<&[u8]> {
        match self {
            DispatchMessage::Remote(message) => message.content(),
            DispatchMessage::Local { .. } => None,
        }
    }
}

type ErrorRepr = (&'static str, BoxError);

#[derive(Debug, Default)]
pub(crate) struct DispatchError(Vec<ErrorRepr>);

impl DispatchError {
    fn add<E>(&mut self, handler_type: &'static str, err: E)
    where
        E: Into<BoxError>,
    {
        self.0.push((handler_type, err.into()));
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, error) in self.0.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}: {}", error.0, error.1)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DispatchOutput {
    /// [`MessageTypeDescriptor`] descriptor of the message that has been dispatched
    pub(crate) descriptor: MessageTypeDescriptor,

    /// Optional id of the message that has been handled
    /// When `None`, indicates that the message has been handled locally and thus does have an associated id
    pub(crate) message_id: Option<MessageId>,

    /// Flag indicating whether the handled message has been sent to the persistence
    pub(crate) persisted: bool,

    /// The originator of the message
    pub(crate) originator: Peer,

    /// The [`MessageExecutionCompleted`] to send back to the originator peer
    pub(crate) completed: Option<MessageExecutionCompleted>,

    /// The [`MessageProcessingFailed`] to publish
    pub(crate) failed: Option<MessageProcessingFailed>,
}

/// A [`Result`] representation of a dispatch
pub(crate) type DispatchResult = Result<Option<Response>, DispatchError>;

/// Final representation of a [`TransportMessage`] that has been dispatched
pub(crate) struct Dispatched {
    /// The initial [`DispatchRequest`]
    request: Arc<DispatchRequest>,

    /// [`MessageTypeDescriptor`] descriptor of the message that has been dispatched
    pub(crate) descriptor: MessageTypeDescriptor,

    /// Result of the dispatch
    pub(crate) result: DispatchResult,
}

#[allow(dead_code)]
impl Dispatched {
    pub(crate) fn is<M: MessageDescriptor + 'static>(&self) -> bool {
        self.descriptor.is::<M>()
    }

    /// Returns `true` if a [`Command`] has been dispatched
    pub(crate) fn is_command(&self) -> bool {
        self.is_kind(MessageKind::Command)
    }

    /// Returns `true` if an [`Event`] has been dispatched
    pub(crate) fn is_event(&self) -> bool {
        self.is_kind(MessageKind::Event)
    }

    pub(crate) fn is_kind(&self, kind: MessageKind) -> bool {
        self.descriptor.kind == kind
    }
}

impl TryInto<CommandResult> for Dispatched {
    type Error = ();

    fn try_into(self) -> Result<CommandResult, Self::Error> {
        self.is_command()
            .then(|| match self.result {
                Ok(None) => Ok(None),
                Ok(Some(response)) => match response {
                    Response::Message(raw) => {
                        let (message_type, payload) = raw.into();
                        Ok(Some(RawMessage::new(message_type, payload)))
                    }
                    Response::Error(code, message) => Err(CommandError::Command {
                        code,
                        message: Some(message),
                    }),
                    Response::StandardError(e) => Err(CommandError::Command {
                        code: HANDLER_ERROR_CODE,
                        message: Some(e.to_string()),
                    }),
                },
                Err(e) => Err(CommandError::Command {
                    code: HANDLER_ERROR_CODE,
                    message: Some(e.to_string()),
                }),
            })
            .ok_or(())
    }
}

impl Into<DispatchOutput> for Dispatched {
    fn into(self) -> DispatchOutput {
        let is_command = self.is_command();

        match &self.request.message {
            DispatchMessage::Remote(ref message) => {
                let originator = message.originator.clone();
                let command_id = message.id.clone();

                // Get originator peer from `OriginatorInfo` of `TransportMessage`
                let peer = originator.into();

                // Create `MessagePocessingFailed` message if dispatch triggered error
                let failed = if let Err(dispatch_error) = self.result.as_ref() {
                    let transport_message = message.clone().into_protobuf();
                    let now_utc = chrono::Utc::now();
                    let failing_handlers =
                        dispatch_error.0.iter().map(|e| e.0.to_string()).collect();

                    Some(MessageProcessingFailed {
                        transport_message,
                        // TODO(oktal): serialize message to JSON
                        message_json: String::new(),
                        exception_message: dispatch_error.to_string(),
                        exception_timestamp_utc: now_utc.into_protobuf(),
                        failing_handlers,
                    })
                } else {
                    None
                };

                // Create `MessageExecutionCompleted` if the dispatched message was a `Command`
                let completed = if is_command {
                    Some(match self.result {
                        Ok(Some(response)) => response.into_message(command_id),
                        Ok(None) => MessageExecutionCompleted {
                            command_id: command_id.into_protobuf(),
                            error_code: 0,
                            payload_type_id: None,
                            payload: None,
                            response_message: None,
                        },
                        Err(e) => MessageExecutionCompleted {
                            command_id: command_id.into_protobuf(),
                            error_code: HANDLER_ERROR_CODE,
                            payload_type_id: None,
                            payload: None,
                            response_message: Some(e.to_string()),
                        },
                    })
                } else {
                    None
                };

                DispatchOutput {
                    descriptor: self.descriptor,
                    message_id: Some(command_id),
                    persisted: message.was_persisted,
                    originator: peer,
                    completed,
                    failed,
                }
            }

            DispatchMessage::Local {
                self_peer,
                environment,
                message,
            } => {
                // Create `MessagePocessingFailed` message if dispatch triggered error
                let failed = if let Err(dispatch_error) = self.result.as_ref() {
                    let (_, transport_message) =
                        TransportMessage::create(&self_peer, environment.clone(), message.as_ref());
                    let transport_message = transport_message.into_protobuf();

                    let now_utc = chrono::Utc::now();
                    let failing_handlers =
                        dispatch_error.0.iter().map(|e| e.0.to_string()).collect();

                    Some(MessageProcessingFailed {
                        transport_message,
                        // TODO(oktal): serialize message to JSON
                        message_json: String::new(),
                        exception_message: dispatch_error.to_string(),
                        exception_timestamp_utc: now_utc.into_protobuf(),
                        failing_handlers,
                    })
                } else {
                    None
                };

                DispatchOutput {
                    descriptor: self.descriptor,
                    message_id: None,
                    // NOTE(oktal): locally dispatched messages are not persisted
                    persisted: false,
                    originator: self_peer.clone(),
                    completed: None,
                    failed,
                }
            }
        }
    }
}

/// A type that encapsulates a request to dispatch a [`DispatchMessage`]
pub struct DispatchRequest {
    message: DispatchMessage,
    bus: Arc<dyn Bus>,
}

impl DispatchRequest {
    /// Create a [`DispatchRequest`] for a [`TransportMessage`] received from a remote peer
    pub(crate) fn remote(message: TransportMessage, bus: Arc<dyn Bus>) -> Self {
        Self {
            message: DispatchMessage::Remote(message),
            bus,
        }
    }

    /// Create a [`DispatchRequest`] for a local [`Message`]
    pub(crate) fn local(
        self_peer: Peer,
        environment: String,
        message: Arc<dyn Message>,
        bus: Arc<dyn Bus>,
    ) -> Self {
        Self {
            message: DispatchMessage::Local {
                self_peer,
                environment,
                message,
            },
            bus,
        }
    }

    pub(crate) fn message(&self) -> &DispatchMessage {
        &self.message
    }

    pub(crate) fn message_type(&self) -> &str {
        self.message
            .message_type()
            .expect("A dispatch message should have a message type")
    }

    pub(crate) fn bus(&self) -> Arc<dyn Bus> {
        Arc::clone(&self.bus)
    }
}

/// A task to dispatch
struct Task {
    ctx: DispatchContext,
    future: BoxFuture<'static, Result<Option<Response>, Infallible>>,

    message: MessageTypeDescriptor,
    dispatch_queue: &'static str,
    handler_type: &'static str,
}

impl Task {
    async fn invoke(self) {
        let response = self
            .future
            .await
            .expect("handler invocation is infaillible");

        self.ctx
            .send(HandlerResponse::new(self.handler_type, response))
            .await
    }
}

/// Trait for a service able to dispatch a [`DispatchRequest`]
pub(crate) trait DispatchService: Service<DispatchRequest> {
    /// A list of [`MessageInvokerDescriptor`] that the service can dispatch
    fn descriptors(&self) -> Result<Vec<MessageInvokerDescriptor>, Self::Error>;
}
