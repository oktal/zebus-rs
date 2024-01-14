mod context;
mod dispatcher;
mod future;
mod invoker;
mod queue;
pub mod router;

use std::convert::Infallible;
use std::{fmt::Display, sync::Arc};

use crate::bus::{CommandError, CommandResult};
use crate::core::RawMessage;
use crate::core::{response::HANDLER_ERROR_CODE, Response};
use crate::lotus::MessageProcessingFailed;
use crate::proto::IntoProtobuf;
use crate::{
    transport::{MessageExecutionCompleted, TransportMessage},
    BoxError, MessageKind, Peer,
};
use crate::{Bus, Message, MessageDescriptor};
pub(crate) use context::DispatchContext;
pub(crate) use dispatcher::{Error, MessageDispatcher};
use futures_core::future::BoxFuture;
pub use invoker::InvokerService;
pub(crate) use invoker::{HandlerResponse, MessageInvokerDescriptor};
use tower_service::Service;
use zebus_core::MessageTypeDescriptor;

/// A message to dispatch
#[derive(Debug, Clone)]
pub(crate) enum DispatchMessage {
    /// Dispatch a [`TransportMessage`] from a remote peer
    Remote(TransportMessage),

    /// Dispatch a local [`Message`]
    Local(Arc<dyn Message>),
}

impl DispatchMessage {
    fn message_type(&self) -> &str {
        match self {
            DispatchMessage::Remote(message) => message.message_type_id.full_name.as_str(),
            DispatchMessage::Local(message) => message.name(),
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

#[derive(Debug, Default)]
pub(crate) struct DispatchOutput {
    /// The originator of the message
    pub(crate) originator: Option<Peer>,

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
    descriptor: MessageTypeDescriptor,

    /// Result of the dispatch
    result: DispatchResult,
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

        if let DispatchMessage::Remote(ref message) = self.request.message {
            let originator = message.originator.clone();
            let command_id = message.id.clone();

            // Get originator peer from `OriginatorInfo` of `TransportMessage`
            let peer = Peer {
                id: originator.sender_id,
                endpoint: originator.sender_endpoint,
                is_up: true,
                is_responding: true,
            };

            // Create `MessagePocessingFailed` message if dispatch triggered error
            let failed = if let Err(dispatch_error) = self.result.as_ref() {
                let now_utc = chrono::Utc::now();
                let failing_handlers = dispatch_error.0.iter().map(|e| e.0.to_string()).collect();

                Some(MessageProcessingFailed {
                    transport_message: message.clone(),
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
                        command_id,
                        error_code: 0,
                        payload_type_id: None,
                        payload: None,
                        response_message: None,
                    },
                    Err(e) => MessageExecutionCompleted {
                        command_id,
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
                originator: Some(peer),
                failed,
                completed,
            }
        } else {
            DispatchOutput::default()
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
    pub(crate) fn local(message: Arc<dyn Message>, bus: Arc<dyn Bus>) -> Self {
        Self {
            message: DispatchMessage::Local(message),
            bus,
        }
    }

    pub(crate) fn message(&self) -> &DispatchMessage {
        &self.message
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
