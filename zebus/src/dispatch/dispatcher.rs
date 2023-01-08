use std::collections::HashMap;

use thiserror::Error;

use super::{
    future::DispatchFuture, queue::DispatchQueue, registry::Registry, Dispatch, Dispatcher,
    MessageDispatch,
};
use crate::{transport::TransportMessage, DispatchHandler};

/// Errors that can be returned by the [`MessageDispatcher`]
#[derive(Debug, Error)]
pub enum Error {
    /// An operation on the [`DispatchQueue`] returned an eror
    #[error("dispatch queue error {0}")]
    Queue(super::queue::Error),

    /// Attempted to register a handler for a message to a different dispatch queue
    #[error("attempted to register message {message_type} to a different dispatch queue {dispatch_queue}, was previously registered to {previous}")]
    DoubleRegister {
        message_type: &'static str,
        dispatch_queue: &'static str,
        previous: &'static str,
    },

    /// An operation was attempted while the [`MessageDispatcher`] was in an invalid state for the
    /// operation
    #[error("an operation was attempted while the dispatcher was not in a valid state")]
    InvalidOperation,
}

/// Represents the type of dispatch to use for a given [`TransportMessage`]
/// Messages that have been marked with a special `infrastructure` attribute will
/// be dispatched synchronously
///
/// Other types of messages will be dispatched asynchronously through a named
/// [`DispatchQueue`]
enum DispatchType {
    Sync(Vec<Box<dyn Dispatch + Send>>),
    Async(DispatchQueue),
}

impl DispatchType {
    fn add<H>(&mut self, registry: Box<Registry<H>>) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        match self {
            DispatchType::Sync(dispatch) => Ok(dispatch.push(registry)),
            DispatchType::Async(queue) => queue.add(registry).map_err(Error::Queue),
        }
    }

    fn start(&mut self) -> Result<(), Error> {
        if let Self::Async(queue) = self {
            queue.start().map_err(Error::Queue)?;
        }

        Ok(())
    }

    fn dispatch(&mut self, message_dispatch: MessageDispatch) -> Result<(), Error> {
        match self {
            Self::Sync(sync) => {
                sync.dispatch(&message_dispatch);
                message_dispatch.set_completed();
                Ok(())
            }
            Self::Async(queue) => queue.send(message_dispatch).map_err(Error::Queue),
        }
    }
}

/// A collection of indexed dispatchers by [`MessageTypeId`]
struct DispatchMap {
    entries: HashMap<&'static str, DispatchType>,
    message_entries: HashMap<&'static str, &'static str>,
}

impl DispatchMap {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            message_entries: HashMap::new(),
        }
    }

    fn add<H>(&mut self, registry: Box<Registry<H>>) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        // A registry might contain mixed handlers of synchronous
        // (messages with `infrastructure` attribute) and asynchronous
        // messages.
        // In such a scenario, that means that some messages, for a same
        // instance of a `DispatcherHandler` should be dispatched synchronously
        // while other messages should be dispatched asynchronously through a
        // `DispatchQueue`.
        // Since we need to transfer ownership of the underlying instance of the
        // `DispatchHandler` handler to the `DispatchQueue`, we *ALSO* need to keep
        // an instance for synchronous dispatching.
        // We thus "split" the registry in half and regroup the handlers based on their dispatch
        // type (asynchronous or synchronous).
        // This will in turn make the instance of handlers share-able between threads
        let (async_registry, sync_registry) =
            registry.split_half(|descriptor| descriptor.is_infrastructure);

        if let Some(async_registry) = async_registry {
            self.add_with(Box::new(async_registry), H::DISPATCH_QUEUE, |name| {
                DispatchType::Async(DispatchQueue::new(name.to_string()))
            })?;
        }

        if let Some(sync_registry) = sync_registry {
            self.add_with(Box::new(sync_registry), "__zebus_internal", |_| {
                DispatchType::Sync(vec![])
            })?;
        }

        Ok(())
    }

    fn start(&mut self) -> Result<(), Error> {
        for (_, entry) in &mut self.entries {
            entry.start()?;
        }

        Ok(())
    }

    fn add_with<H>(
        &mut self,
        registry: Box<Registry<H>>,
        dispatch_queue: &'static str,
        factory: impl FnOnce(&'static str) -> DispatchType,
    ) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        // Register all handled messages to make sure the user did not attempt to register a message handler to a different dispatch queue
        for handled_message in registry.handled_messages() {
            match self.message_entries.entry(handled_message) {
                std::collections::hash_map::Entry::Occupied(e) => Err(Error::DoubleRegister {
                    message_type: handled_message,
                    dispatch_queue,
                    previous: e.key(),
                }),
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(dispatch_queue);
                    Ok(())
                }
            }?;
        }

        // Create the dispatch entry if it does not exist and add the registry
        self.entries
            .entry(dispatch_queue)
            .or_insert(factory(dispatch_queue))
            .add(registry)
    }

    fn dispatch(&mut self, message_dispatch: MessageDispatch) -> Result<(), Error> {
        let entry = self
            .message_entries
            .get_mut(message_dispatch.message.message_type_id.full_name.as_str())
            .and_then(|dispatch_entry| self.entries.get_mut(dispatch_entry));

        entry
            .ok_or(Error::InvalidOperation)
            .and_then(|e| e.dispatch(message_dispatch))
    }
}

/// Inner state of the dispatcher
enum Inner {
    /// Initialized state
    Init(DispatchMap),

    /// Started state
    Started(DispatchMap),
}

/// Dispatcher based on [`MessageTypeId`]
pub(crate) struct MessageDispatcher {
    inner: Option<Inner>,
}

impl MessageDispatcher {
    /// Create a new empty dispatcher
    pub(crate) fn new() -> Self {
        Self {
            inner: Some(Inner::Init(DispatchMap::new())),
        }
    }

    /// Add a [`Registry`] of handlers to the dispatcher
    pub(crate) fn add<H>(&mut self, registry: Box<Registry<H>>) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        match self.inner.as_mut() {
            Some(Inner::Init(ref mut dispatch)) => dispatch.add(registry),
            _ => Err(Error::InvalidOperation),
        }
    }

    /// Start the [`MessageDispatcher`]. This will start all the registered [`DispatchQueue`]
    /// queues
    pub(crate) fn start(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init(mut dispatch)) => {
                // Start all the dispatchers
                dispatch.start()?;

                // Transition to Started state
                (Some(Inner::Started(dispatch)), Ok(()))
            }
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    fn dispatch(&mut self, message_dispatch: MessageDispatch) -> Result<(), Error> {
        let dispatch_map = match self.inner.as_mut() {
            Some(Inner::Init(map)) | Some(Inner::Started(map)) => Ok(map),
            _ => Err(Error::InvalidOperation),
        }?;

        dispatch_map.dispatch(message_dispatch)
    }
}

impl Dispatcher for MessageDispatcher {
    fn dispatch(&mut self, message: TransportMessage) -> DispatchFuture {
        let (dispatch, future) = MessageDispatch::new(message);
        // TODO(oktal): correctly handle underlying result
        self.dispatch(dispatch).unwrap();
        future
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::MessagePayload,
        dispatch::{registry, DispatchResult},
        Handler, HandlerError, MessageKind, Peer, Response, ResponseMessage,
    };

    struct Fixture {
        dispatcher: MessageDispatcher,
        peer: Peer,
        environment: String,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dispatcher: MessageDispatcher::new(),
                peer: Peer::test(),
                environment: "Test".to_string(),
            }
        }

        fn add<H>(&mut self, handler: Box<H>, registry_fn: impl FnOnce(&mut Registry<H>))
        where
            H: DispatchHandler + Send + 'static,
        {
            self.dispatcher
                .add(registry::for_handler(handler, registry_fn))
                .expect("failed to add handler");
        }

        fn start(&mut self) -> Result<(), Error> {
            self.dispatcher.start()
        }

        fn dispatch<M>(&mut self, message: &M) -> Result<DispatchFuture, Error>
        where
            M: crate::Message + prost::Message,
        {
            let (_, message) =
                TransportMessage::create(&self.peer, self.environment.clone(), message);
            let (message_dispatch, future) = MessageDispatch::new(message);
            self.dispatcher.dispatch(message_dispatch)?;
            Ok(future)
        }

        fn decode_as<M: crate::Message + prost::Message + Default>(
            result: &DispatchResult,
        ) -> Option<M> {
            if let Ok(Some(response)) = result {
                if let Response::Message(message) = response {
                    return message.decode_as::<M>().and_then(|r| r.ok());
                }
            }

            None
        }
    }

    #[derive(crate::Handler)]
    #[zebus(dispatch_queue = "CustomQueue")]
    struct HandlerWithCustomDispatchQueue {}

    #[derive(crate::Handler)]
    struct HandlerWithDefaultDispatchQueue {}

    #[derive(prost::Message, crate::Command)]
    #[zebus(namespace = "Abc.Test")]
    struct ParseCommand {
        #[prost(string, required, tag = 1)]
        value: String,
    }

    #[derive(prost::Message, crate::Command)]
    #[zebus(namespace = "Abc.Test")]
    #[derive(Eq, PartialEq)]
    struct ParseResponse {
        #[prost(sint64, required, tag = 1)]
        value: i64,
    }

    #[derive(Debug, Error)]
    enum ParseError {
        #[error("negative number can not be parsed")]
        Negative,
    }

    impl crate::Error for ParseError {
        fn code(&self) -> i32 {
            1
        }
    }

    #[derive(crate::Handler)]
    struct ParseCommandHandler;

    #[derive(crate::Handler)]
    struct ParseCommandResponseErrorHandler;

    impl Handler<ParseCommand> for ParseCommandHandler {
        type Response = ();

        fn handle(&mut self, _message: ParseCommand) {}
    }

    impl Handler<ParseCommand> for ParseCommandResponseErrorHandler {
        type Response = Result<ResponseMessage<ParseResponse>, HandlerError<ParseError>>;

        fn handle(&mut self, message: ParseCommand) -> Self::Response {
            let mut chars = message.value.chars();
            if let Some(first) = chars.next() {
                if first == '-' {
                    return Err(HandlerError::User(ParseError::Negative));
                }
            }

            Ok(ParseResponse {
                value: message.value.parse::<i64>()?,
            }
            .into())
        }
    }

    #[test]
    fn custom_dispatch_queue() {
        assert_eq!(
            <HandlerWithCustomDispatchQueue as DispatchHandler>::DISPATCH_QUEUE,
            "CustomQueue"
        );
    }

    #[test]
    fn default_dispatch_queue() {
        assert_eq!(
            <HandlerWithDefaultDispatchQueue as DispatchHandler>::DISPATCH_QUEUE,
            zebus_core::DEFAULT_DISPATCH_QUEUE
        );
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_no_response() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch(&command)
            .expect("failed to dispatch command")
            .await;
        assert_eq!(dispatched.kind, MessageKind::Command);
        assert!(matches!(dispatched.result, Ok(None)));
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_response() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch(&command)
            .expect("failed to dispatch command")
            .await;
        let response = Fixture::decode_as::<ParseResponse>(&dispatched.result);
        assert_eq!(dispatched.kind, MessageKind::Command);
        assert_eq!(response, Some(ParseResponse { value: 987612 }));
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_user_error() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "-43".to_string(),
        };
        let dispatched = fixture
            .dispatch(&command)
            .expect("failed to dispatch command")
            .await;

        let expected_error = Response::Error(1, format!("{}", ParseError::Negative));

        assert!(matches!(
            dispatched.result.ok().flatten(),
            Some(expected_error)
        ));
        assert_eq!(dispatched.kind, MessageKind::Command);
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_standard_error() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "NotANumber".to_string(),
        };
        let dispatched = fixture
            .dispatch(&command)
            .expect("failed to dispatch command")
            .await;

        let err = dispatched.result.unwrap_err();

        assert_eq!(err.count(), 1);
        assert_eq!(dispatched.kind, MessageKind::Command);
    }
}
