use std::collections::HashMap;

use thiserror::Error;

use super::{
    future::DispatchFuture, queue::DispatchQueue, registry::Registry, Dispatch, DispatchContext,
    Dispatcher, MessageDispatch,
};
use crate::MessageTypeDescriptor;
use crate::{core::SubscriptionMode, BindingKey, DispatchHandler};

/// Errors that can be returned by the [`MessageDispatcher`]
#[derive(Debug, Error)]
pub enum Error {
    /// An operation on the [`DispatchQueue`] returned an eror
    #[error("dispatch queue error {0}")]
    Queue(super::queue::Error),

    /// No dispatcher could be found for a given message
    #[error("could not find dispatcher for message {0}")]
    DispatcherNotFound(String),

    /// A message handler was already registered with a different [`SubscriptionMode`]
    #[error("attempted to register message {0} with a different subscription mode")]
    SubscriptionModeConflict(String),

    /// A message handler was already registered with a different dispatch queue
    #[error("attempted to register message {message_type} to dispatch queue {dispatch_queue}. Already registered to {previous_dispatch_queue}")]
    DispatchQueueConflict {
        message_type: String,
        dispatch_queue: &'static str,
        previous_dispatch_queue: &'static str,
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
enum DispatchFlavor {
    /// A synchronous dispatcher that will call the message handler directly from the context of
    /// the calling thread
    Sync(Vec<Box<dyn Dispatch + Send>>),

    /// An asynchronous dispatcher that will call the message handler from the context of a
    /// [`DispatchQueue`]
    Async(DispatchQueue),
}

impl DispatchFlavor {
    fn add<H>(&mut self, registry: Box<Registry<H>>) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        match self {
            DispatchFlavor::Sync(dispatch) => Ok(dispatch.push(registry)),
            DispatchFlavor::Async(queue) => queue.add(registry).map_err(Error::Queue),
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

/// Dispatch information for a [`Message`]
struct MessageDispatchInfo {
    /// The descriptor of the message to dispatch
    descriptor: MessageTypeDescriptor,

    /// Startup subscription mode
    subscription_mode: SubscriptionMode,

    /// Startup subscription bindings
    bindings: Vec<BindingKey>,
}

/// A collection of indexed dispatchers by [`MessageTypeId`]
struct DispatchMap {
    /// Maps a named dispatch queue to the associated dispatcher
    message_dispatchers: HashMap<&'static str, DispatchFlavor>,

    /// Maps a [`MessageTypeId`] to a named dispatch queue
    message_dispatch_queues: HashMap<String, &'static str>,

    /// Maps a [`MessageTypeId`] to its associated dispatching information
    dispatch_info: HashMap<String, MessageDispatchInfo>,
}

impl DispatchMap {
    fn new() -> Self {
        Self {
            message_dispatchers: HashMap::new(),
            message_dispatch_queues: HashMap::new(),
            dispatch_info: HashMap::new(),
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
                DispatchFlavor::Async(DispatchQueue::new(name.to_string()))
            })?;
        }

        if let Some(sync_registry) = sync_registry {
            self.add_with(Box::new(sync_registry), "__zebus_internal", |_| {
                DispatchFlavor::Sync(vec![])
            })?;
        }

        Ok(())
    }

    fn start(&mut self) -> Result<(), Error> {
        for dispatcher in &mut self.message_dispatchers.values_mut() {
            dispatcher.start()?;
        }

        Ok(())
    }

    fn add_with<H>(
        &mut self,
        registry: Box<Registry<H>>,
        dispatch_queue: &'static str,
        factory: impl FnOnce(&'static str) -> DispatchFlavor,
    ) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        for (handled_message, subscription_mode, bindings) in registry.handled_messages() {
            // Attempt to register a message to its dispatch queue and make sure it was not already
            // registered with an other dispatch queue
            match self
                .message_dispatch_queues
                .entry(handled_message.full_name.clone())
            {
                // The message is already registered with an other dispatch queue
                std::collections::hash_map::Entry::Occupied(e) => {
                    Err(Error::DispatchQueueConflict {
                        message_type: handled_message.full_name.clone(),
                        dispatch_queue,
                        previous_dispatch_queue: e.get(),
                    })
                }
                // Insert the message
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(dispatch_queue);
                    Ok(())
                }
            }?;

            // Register the message dispatching information
            let dispatch_info = self
                .dispatch_info
                .entry(handled_message.full_name.clone())
                .or_insert(MessageDispatchInfo {
                    descriptor: handled_message.clone(),
                    subscription_mode,
                    bindings: vec![],
                });

            // Make sure the startup subscription does not conflict with a previous registration
            if dispatch_info.subscription_mode != subscription_mode {
                return Err(Error::SubscriptionModeConflict(
                    handled_message.full_name.clone(),
                ));
            }

            // Add the startup bindings to the dispatching information
            dispatch_info.bindings.extend_from_slice(bindings);
        }

        // Create the dispatcher if it does not exist and add the registry to it
        self.message_dispatchers
            .entry(dispatch_queue)
            .or_insert(factory(dispatch_queue))
            .add(registry)
    }

    fn dispatch(&mut self, message_dispatch: MessageDispatch) -> Result<(), Error> {
        let dispatcher = self
            .message_dispatch_queues
            .get_mut(message_dispatch.message_type())
            .and_then(|dispatch_entry| self.message_dispatchers.get_mut(dispatch_entry));

        dispatcher
            .ok_or(Error::DispatcherNotFound(
                message_dispatch.message_type().to_string(),
            ))
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

    pub(crate) fn handled_mesages<'a>(
        &'a self,
    ) -> Result<
        impl Iterator<
            Item = (
                &'a MessageTypeDescriptor,
                SubscriptionMode,
                &'a [BindingKey],
            ),
        >,
        Error,
    > {
        match self.inner.as_ref() {
            Some(Inner::Init(ref dispatch)) | Some(Inner::Started(ref dispatch)) => Ok(dispatch
                .dispatch_info
                .iter()
                .map(|(_message_type, dispatch_info)| {
                    (
                        &dispatch_info.descriptor,
                        dispatch_info.subscription_mode,
                        &dispatch_info.bindings[..],
                    )
                })),
            _ => Err(Error::InvalidOperation),
        }
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
    fn dispatch(&mut self, context: DispatchContext) -> DispatchFuture {
        let (dispatch, future) = MessageDispatch::new(context);
        // TODO(oktal): correctly handle underlying result
        self.dispatch(dispatch).unwrap();
        future
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Arc;

    use super::*;
    use crate::{
        bus::{self, CommandResult},
        core::{HandlerDescriptor, MessagePayload},
        dispatch::{registry, DispatchRequest, DispatchResult},
        handler,
        transport::TransportMessage,
        Bus, Command, Event, Handler, HandlerError, Message, MessageDescriptor, MessageKind, Peer,
        PeerId, Response, ResponseMessage,
    };

    use zebus_core::binding_key;

    struct TestBus;

    #[async_trait]
    impl Bus for TestBus {
        fn configure(&self, _peer_id: PeerId, _environment: String) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn start(&self) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn stop(&self) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn send(&self, _command: &dyn Command) -> bus::Result<crate::bus::CommandFuture> {
            Err(bus::Error::InvalidOperation)
        }

        async fn send_to(
            &self,
            _command: &dyn Command,
            _peer: Peer,
        ) -> bus::Result<crate::bus::CommandFuture> {
            Err(bus::Error::InvalidOperation)
        }

        async fn publish(&self, _event: &dyn Event) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }
    }

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

        async fn send<M>(&mut self, message: M) -> Result<CommandResult, Error>
        where
            M: crate::Message + prost::Message + 'static,
        {
            let dispatched = self.dispatch_local(message)?.await;
            Ok(dispatched.try_into().expect("missing CommandResult"))
        }

        fn dispatch<M>(&mut self, message: &M) -> Result<DispatchFuture, Error>
        where
            M: crate::Message + prost::Message,
        {
            let (_, message) =
                TransportMessage::create(&self.peer, self.environment.clone(), message);
            let request = DispatchRequest::Remote(message);
            let bus = Arc::new(TestBus);
            let context = DispatchContext::new(request, bus);
            let (message_dispatch, future) = MessageDispatch::new(context);
            self.dispatcher.dispatch(message_dispatch)?;
            Ok(future)
        }

        fn dispatch_local<M>(&mut self, message: M) -> Result<DispatchFuture, Error>
        where
            M: Message,
        {
            let request = DispatchRequest::Local(Arc::new(message));
            let bus = Arc::new(TestBus);
            let context = DispatchContext::new(request, bus);
            let (message_dispatch, future) = MessageDispatch::new(context);
            self.dispatcher.dispatch(message_dispatch)?;
            Ok(future)
        }

        fn decode_as<M: MessageDescriptor + prost::Message + Default>(
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

    #[derive(crate::Command, prost::Message)]
    #[zebus(namespace = "Abc.Test")]
    struct AutoSubscribeCommand {
        #[prost(uint32, tag = 1)]
        id: u32,
    }

    #[derive(crate::Command, prost::Message)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct AutoSubscribeCommandWithRouting {
        #[prost(string, tag = 1)]
        #[zebus(routing_position = 1)]
        month: String,
    }

    #[derive(crate::Command, prost::Message)]
    #[zebus(namespace = "Abc.Test")]
    struct ManualSubscribeCommand {
        #[prost(uint32, tag = 1)]
        id: u32,
    }

    #[derive(crate::Handler)]
    struct TestHandler {}

    #[handler(auto)]
    impl crate::Handler<AutoSubscribeCommand> for TestHandler {
        type Response = ();

        fn handle(&mut self, _message: AutoSubscribeCommand) -> Self::Response {}
    }

    #[handler(auto, binding = "october")]
    impl crate::Handler<AutoSubscribeCommandWithRouting> for TestHandler {
        type Response = ();

        fn handle(&mut self, _message: AutoSubscribeCommandWithRouting) -> Self::Response {}
    }

    #[handler(manual)]
    impl crate::Handler<ManualSubscribeCommand> for TestHandler {
        type Response = ();

        fn handle(&mut self, _message: ManualSubscribeCommand) -> Self::Response {}
    }

    #[derive(prost::Message, crate::Command, Clone)]
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

    #[handler(manual)]
    impl Handler<ParseCommand> for ParseCommandHandler {
        type Response = ();

        fn handle(&mut self, _message: ParseCommand) {}
    }

    #[handler(manual)]
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

    #[test]
    fn dispatch_auto_subscribe() {
        assert_eq!(
            <TestHandler as HandlerDescriptor<AutoSubscribeCommand>>::subscription_mode(),
            SubscriptionMode::Auto
        )
    }

    #[test]
    fn dispatch_manual_subscribe() {
        assert_eq!(
            <TestHandler as HandlerDescriptor<ManualSubscribeCommand>>::subscription_mode(),
            SubscriptionMode::Manual
        )
    }

    #[test]
    fn dispatch_subscribe_bindings() {
        assert_eq!(
            <TestHandler as HandlerDescriptor<AutoSubscribeCommandWithRouting>>::bindings(),
            vec![binding_key!["october"]]
        )
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

    #[tokio::test]
    async fn dispatch_local_message_to_handler_with_no_response() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_local(command)
            .expect("failed to dispatch command")
            .await;
        assert_eq!(dispatched.kind, MessageKind::Command);
        assert!(matches!(dispatched.result, Ok(None)));
    }

    #[tokio::test]
    async fn dispatch_local_message_to_handler_with_response() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_local(command)
            .expect("failed to dispatch command")
            .await;
        let response = Fixture::decode_as::<ParseResponse>(&dispatched.result);
        assert_eq!(dispatched.kind, MessageKind::Command);
        assert_eq!(response, Some(ParseResponse { value: 987612 }));
    }

    #[tokio::test]
    async fn dispatch_local_command_with_success_result() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let result = fixture.send(command).await.unwrap();
        assert!(matches!(result, Ok(None)));
    }

    #[tokio::test]
    async fn dispatch_local_command_with_success_result_and_response() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let result = fixture.send(command).await.unwrap();
        let response = result.decode_as::<ParseResponse>();
        assert_eq!(response, Some(Ok(ParseResponse { value: 987612 })));
    }

    #[tokio::test]
    async fn dispatch_local_command_with_error_result() {
        let mut fixture = Fixture::new();
        fixture.add(Box::new(ParseCommandResponseErrorHandler), |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "NotANumber".to_string(),
        };
        let result = fixture.send(command).await.unwrap();
        assert_eq!(result.is_err(), true);
    }
}
