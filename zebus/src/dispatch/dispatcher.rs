use std::collections::HashMap;

use thiserror::Error;

use super::future::DispatchFuture;
use super::queue::DispatchQueue;
use super::{
    DispatchJob, DispatchRequest, DispatchService, HandlerInvoker, MessageInvokerDescriptor,
};
use crate::MessageTypeDescriptor;

/// Errors that can be returned by the [`MessageDispatcher`]
#[derive(Debug, Error)]
pub enum Error {
    /// An operation on the [`DispatchQueue`] returned an eror
    #[error("dispatch queue error {0}")]
    Queue(super::queue::Error),

    /// No dispatcher could be found for a given message
    #[error("unknown message {0}")]
    UnknownMessage(String),

    /// Responses for handler invocation are missing
    #[error("missing {0} responses for handler invocation")]
    Missing(usize),

    /// An operation was attempted while the [`MessageDispatcher`] was in an invalid state for the
    /// operation
    #[error("an operation was attempted while the dispatcher was not in a valid state")]
    InvalidOperation,
}

/// Descriptor for a specific type of message
struct MessageTypeInvokerDescriptor {
    /// Descriptor of the message
    message: MessageTypeDescriptor,

    /// List of invoker descriptors for this message
    invokers: Vec<MessageInvokerDescriptor>,
}

impl MessageTypeInvokerDescriptor {
    fn new(message: MessageTypeDescriptor) -> Self {
        Self {
            message,
            invokers: vec![],
        }
    }
}

struct InvokerDispatcher {
    /// Dispatch queues indexed by their name
    dispatch_queues: HashMap<&'static str, DispatchQueue>,

    /// Descriptors of invokers  for messages indexed by their message type
    descriptors: HashMap<&'static str, MessageTypeInvokerDescriptor>,

    /// List of invokers
    invokers: Vec<Box<dyn HandlerInvoker>>,
}

impl InvokerDispatcher {
    fn new() -> Self {
        Self {
            dispatch_queues: HashMap::new(),
            descriptors: HashMap::new(),
            invokers: Vec::new(),
        }
    }

    fn add(&mut self, invoker: Box<dyn HandlerInvoker>) -> Result<(), Error> {
        for descriptor in invoker.descriptors() {
            let dispatch_queue = descriptor.dispatch_queue;

            let dispatch_info = self
                .descriptors
                .entry(descriptor.message.full_name)
                .or_insert(MessageTypeInvokerDescriptor::new(
                    descriptor.message.clone(),
                ));
            dispatch_info.invokers.push(descriptor);

            self.dispatch_queues
                .entry(dispatch_queue)
                .or_insert_with(|| DispatchQueue::new(dispatch_queue.to_string()));
        }

        self.invokers.push(invoker);
        Ok(())
    }

    fn start(&mut self) -> Result<(), Error> {
        for (_name, dipatch_queue) in &mut self.dispatch_queues {
            dipatch_queue.start().map_err(Error::Queue)?;
        }

        Ok(())
    }

    fn dispatch(&mut self, request: DispatchRequest) -> DispatchFuture {
        // Retrieve the descriptor for the message we are about to dispatch and
        // return an error'd future if we failed to find it
        let descriptor = match self.descriptors.get(request.message.message_type()) {
            Some(descriptor) => descriptor,
            None => {
                return super::future::err(Error::UnknownMessage(
                    request.message.message_type().to_string(),
                ))
            }
        };

        // Create the dispatch context
        let (context, rx) = super::context::new(request);

        // Create the invokers of the handlers for the message
        let message_invokers = self
            .invokers
            .iter()
            .filter_map(|i| i.create_invoker(context.clone()));

        let mut total_invokers = 0;

        for message_invoker in message_invokers {
            // Retrieve the descriptor of the message we are about to dispatch
            let descriptor = message_invoker.descriptor;

            // Create the dispatch job
            let job = DispatchJob::new(context.clone(), message_invoker.invoker);

            // For an infrastructure message, spawn the job and start the execution of the invoker right away
            if descriptor.message.is_infrastructure {
                tokio::spawn(job.invoke());
            } else {
                // Retrieve the dispatch queue for the message
                let queue = self
                    .dispatch_queues
                    .get(descriptor.dispatch_queue)
                    .expect("unconfigured dispatch queue");

                // Attempt to enqueue the job and return an error'd future if we failed
                if let Err(e) = queue.enqueue(job) {
                    return super::future::err(Error::Queue(e));
                }
            }

            total_invokers += 1;
        }

        // Create the future that will resolve when all handlers have been called
        super::future::new(
            total_invokers,
            context.request(),
            descriptor.message.clone(),
            rx,
        )
    }
}

/// Inner state of the dispatcher
enum Inner {
    /// Initialized state
    Init(InvokerDispatcher),

    /// Started state
    Started(InvokerDispatcher),
}

/// Dispatcher based on [`MessageTypeId`]
pub(crate) struct MessageDispatcher {
    inner: Option<Inner>,
}

impl MessageDispatcher {
    /// Create a new empty dispatcher
    pub(crate) fn new() -> Self {
        Self {
            inner: Some(Inner::Init(InvokerDispatcher::new())),
        }
    }

    /// Add a [`Registry`] of handlers to the dispatcher
    pub(crate) fn add(&mut self, invoker: Box<dyn HandlerInvoker>) -> Result<(), Error> {
        match self.inner.as_mut() {
            Some(Inner::Init(ref mut dispatch)) => dispatch.add(invoker),
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

    fn descriptors<'a>(
        &'a self,
    ) -> Result<impl Iterator<Item = &'a MessageInvokerDescriptor>, Error> {
        match self.inner.as_ref() {
            Some(Inner::Init(ref dispatch)) | Some(Inner::Started(ref dispatch)) => Ok(dispatch
                .descriptors
                .values()
                .flat_map(|v| v.invokers.iter())),
            _ => Err(Error::InvalidOperation),
        }
    }

    fn dispatch(&mut self, request: DispatchRequest) -> DispatchFuture {
        let dispatcher = match self.inner.as_mut() {
            Some(Inner::Init(dispatcher)) | Some(Inner::Started(dispatcher)) => dispatcher,
            _ => return super::future::err(Error::InvalidOperation),
        };

        dispatcher.dispatch(request)
    }
}

impl DispatchService for MessageDispatcher {
    type Error = Error;
    type Future = DispatchFuture;

    fn descriptors(&self) -> Result<Vec<MessageInvokerDescriptor>, self::Error> {
        self.descriptors().map(|it| it.cloned().collect())
    }

    fn call(&mut self, request: DispatchRequest) -> Self::Future {
        self.dispatch(request)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{
        bus::{self, CommandResult},
        core::{HandlerDescriptor, MessagePayload},
        dispatch::{
            registry::{self, Registry},
            DispatchRequest, DispatchResult, Dispatched,
        },
        handler,
        transport::TransportMessage,
        Bus, Command, Context, DispatchHandler, Event, Handler, HandlerError, Message,
        MessageDescriptor, Peer, PeerId, Response, ResponseMessage, SubscriptionMode,
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

        fn add<H>(&mut self, handler: H, registry_fn: impl FnOnce(&mut Registry<H>))
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
            let dispatched = self.dispatch_local(message).await?;
            Ok(dispatched.try_into().expect("missing CommandResult"))
        }

        async fn dispatch_remote<M>(&mut self, message: &M) -> Result<Dispatched, Error>
        where
            M: crate::Message + prost::Message,
        {
            let (_, message) =
                TransportMessage::create(&self.peer, self.environment.clone(), message);
            self.dispatch(DispatchRequest::remote(message, Arc::new(TestBus)))
                .await
        }

        async fn dispatch_local<M>(&mut self, message: M) -> Result<Dispatched, Error>
        where
            M: Message,
        {
            self.dispatch(DispatchRequest::local(Arc::new(message), Arc::new(TestBus)))
                .await
        }

        async fn dispatch(&mut self, request: DispatchRequest) -> Result<Dispatched, Error> {
            self.dispatcher.dispatch(request).await
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

    struct TrackerHandler<H> {
        tracker: Tracker,
        handler: H,
    }

    fn track<H>(handler: H) -> (TrackerHandler<H>, Tracker) {
        let tracker = Tracker::new();
        (
            TrackerHandler {
                tracker: tracker.clone(),
                handler,
            },
            tracker,
        )
    }

    impl<H, M> HandlerDescriptor<M> for TrackerHandler<H>
    where
        H: HandlerDescriptor<M>,
    {
        fn subscription_mode() -> SubscriptionMode {
            H::subscription_mode()
        }

        fn bindings() -> Vec<zebus_core::BindingKey> {
            H::bindings()
        }
    }

    #[async_trait::async_trait]
    impl<H, M> Handler<M> for TrackerHandler<H>
    where
        H: DispatchHandler + Handler<M> + Send,
        M: Message,
    {
        type Response = H::Response;

        async fn handle(&mut self, message: M, ctx: Context<'_>) -> Self::Response {
            self.tracker.track(&message);
            self.handler.handle(message, ctx).await
        }
    }

    impl<H: DispatchHandler> DispatchHandler for TrackerHandler<H> {
        const DISPATCH_QUEUE: &'static str = H::DISPATCH_QUEUE;
    }

    #[derive(Clone)]
    struct Tracker {
        triggers: Arc<Mutex<HashMap<&'static str, usize>>>,
    }

    impl Tracker {
        fn new() -> Self {
            Self {
                triggers: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn track(&self, message: &dyn crate::core::Message) {
            let mut triggers = self.triggers.lock().unwrap();
            *triggers.entry(message.name()).or_insert(0) += 1;
        }

        fn count_for<M: MessageDescriptor>(&self) -> usize {
            let triggers = self.triggers.lock().unwrap();
            triggers.get(M::name()).cloned().unwrap_or(0usize)
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
    #[async_trait]
    impl crate::Handler<AutoSubscribeCommand> for TestHandler {
        type Response = ();

        async fn handle(
            &mut self,
            _message: AutoSubscribeCommand,
            _ctx: Context<'_>,
        ) -> Self::Response {
        }
    }

    #[handler(binding = "october")]
    #[async_trait]
    impl crate::Handler<AutoSubscribeCommandWithRouting> for TestHandler {
        type Response = ();

        async fn handle(
            &mut self,
            _message: AutoSubscribeCommandWithRouting,
            _ctx: Context<'_>,
        ) -> Self::Response {
        }
    }

    #[handler(manual)]
    #[async_trait]
    impl crate::Handler<ManualSubscribeCommand> for TestHandler {
        type Response = ();

        async fn handle(
            &mut self,
            _message: ManualSubscribeCommand,
            _ctx: Context<'_>,
        ) -> Self::Response {
        }
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
    struct ParseCommandHandler {}

    #[derive(crate::Handler)]
    struct ParseCommandResponseErrorHandler;

    #[handler(manual)]
    #[async_trait]
    impl Handler<ParseCommand> for ParseCommandHandler {
        type Response = ();

        async fn handle(&mut self, _message: ParseCommand, _ctx: Context<'_>) {}
    }

    #[handler(manual)]
    #[async_trait]
    impl Handler<ParseCommand> for ParseCommandResponseErrorHandler {
        type Response = Result<ResponseMessage<ParseResponse>, HandlerError<ParseError>>;

        async fn handle(&mut self, message: ParseCommand, _ctx: Context<'_>) -> Self::Response {
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

    #[derive(prost::Message, crate::Command, Clone)]
    #[zebus(namespace = "Abc.Test", infrastructure)]
    struct PingCommand {
        #[prost(uint64, required, tag = 1)]
        seq: u64,
    }

    #[derive(prost::Message, crate::Command, Clone)]
    #[zebus(namespace = "Abc.Test")]
    struct PongResponse {
        #[prost(uint64, required, tag = 1)]
        seq: u64,
    }

    #[derive(crate::Handler)]
    struct PingCommandHandler {}

    #[handler]
    #[async_trait]
    impl Handler<PingCommand> for PingCommandHandler {
        type Response = ResponseMessage<PongResponse>;

        async fn handle(&mut self, message: PingCommand, _ctx: Context<'_>) -> Self::Response {
            PongResponse { seq: message.seq }.into()
        }
    }

    #[derive(prost::Message, crate::Event, Clone)]
    #[zebus(namespace = "Abc.Test")]
    struct TestSucceeded {
        #[prost(double, required, tag = 1)]
        execution_time_secs: f64,
    }

    #[derive(crate::Handler)]
    #[zebus(dispatch_queue = "PersisenceQueue")]
    struct TestPersistenceHandler;

    #[derive(crate::Handler)]
    #[zebus(dispatch_queue = "PersisenceQueue")]
    struct TestOutputDisplayHandler;

    #[handler]
    #[async_trait]
    impl Handler<TestSucceeded> for TestPersistenceHandler {
        type Response = ();

        async fn handle(&mut self, _message: TestSucceeded, _ctx: Context<'_>) -> Self::Response {}
    }

    #[handler]
    #[async_trait]
    impl Handler<TestSucceeded> for TestOutputDisplayHandler {
        type Response = ();

        async fn handle(&mut self, _message: TestSucceeded, _ctx: Context<'_>) -> Self::Response {}
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
        let (handler, tracker) = track(ParseCommandHandler {});
        fixture.add(handler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_remote(&command)
            .await
            .expect("failed to dispatch command");

        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert!(matches!(dispatched.result, Ok(None)));
        assert_eq!(tracker.count_for::<ParseCommand>(), 1);
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_response() {
        let mut fixture = Fixture::new();
        fixture.add(ParseCommandResponseErrorHandler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_remote(&command)
            .await
            .expect("failed to dispatch command");

        let response = Fixture::decode_as::<ParseResponse>(&dispatched.result);
        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert_eq!(response, Some(ParseResponse { value: 987612 }));
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_user_error() {
        let mut fixture = Fixture::new();
        fixture.add(ParseCommandResponseErrorHandler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "-43".to_string(),
        };
        let dispatched = fixture
            .dispatch_remote(&command)
            .await
            .expect("failed to dispatch command");

        let expected_error = format!("{}", ParseError::Negative);

        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert!(matches!(
            dispatched.result,
            Ok(Some(Response::Error(1, expected_error)))
        ));
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_standard_error() {
        let mut fixture = Fixture::new();
        fixture.add(ParseCommandResponseErrorHandler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "NotANumber".to_string(),
        };
        let dispatched = fixture
            .dispatch_remote(&command)
            .await
            .expect("failed to dispatch command");

        let err = dispatched.result.unwrap_err();

        assert_eq!(err.count(), 1);
        assert!(dispatched.descriptor.is::<ParseCommand>());
    }

    #[tokio::test]
    async fn dispatch_local_message_to_handler_with_no_response() {
        let mut fixture = Fixture::new();
        let (handler, tracker) = track(ParseCommandHandler {});
        fixture.add(handler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_local(command)
            .await
            .expect("failed to dispatch command");

        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert!(matches!(dispatched.result, Ok(None)));
        assert_eq!(tracker.count_for::<ParseCommand>(), 1);
    }

    #[tokio::test]
    async fn dispatch_local_message_to_handler_with_response() {
        let mut fixture = Fixture::new();
        fixture.add(ParseCommandResponseErrorHandler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_local(command)
            .await
            .expect("failed to dispatch command");

        let response = Fixture::decode_as::<ParseResponse>(&dispatched.result);
        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert_eq!(response, Some(ParseResponse { value: 987612 }));
    }

    #[tokio::test]
    async fn dispatch_local_command_with_success_result() {
        let mut fixture = Fixture::new();
        let (handler, tracker) = track(ParseCommandHandler {});
        fixture.add(handler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "987612".to_string(),
        };
        let dispatched = fixture
            .dispatch_local(command)
            .await
            .expect("failed to dispatch command");

        assert!(dispatched.descriptor.is::<ParseCommand>());
        assert!(matches!(dispatched.result, Ok(None)));
        assert_eq!(tracker.count_for::<ParseCommand>(), 1);
    }

    #[tokio::test]
    async fn dispatch_local_command_with_success_result_and_response() {
        let mut fixture = Fixture::new();
        fixture.add(ParseCommandResponseErrorHandler, |h| {
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
        fixture.add(ParseCommandResponseErrorHandler, |h| {
            h.handles::<ParseCommand>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "NotANumber".to_string(),
        };
        let result = fixture.send(command).await.unwrap();
        assert_eq!(result.is_err(), true);
    }

    #[tokio::test]
    async fn dispatch_infrastructure_message() {
        let mut fixture = Fixture::new();
        let (handler, tracker) = track(PingCommandHandler {});
        fixture.add(handler, |h| {
            h.handles::<PingCommand>();
        });
        assert_eq!(fixture.start().is_ok(), true);

        let dispatched = fixture
            .dispatch_remote(&PingCommand { seq: 0xABC })
            .await
            .expect("failed to dispatch command");

        assert!(dispatched.descriptor.is::<PingCommand>());
        let response = Fixture::decode_as::<PongResponse>(&dispatched.result);
        assert!(matches!(response, Some(PongResponse { seq: 0xABC })));
        assert_eq!(tracker.count_for::<PingCommand>(), 1);
    }

    #[tokio::test]
    async fn dispatch_event_to_multiple_handlers_in_multiple_dispatch_queues() {
        let mut fixture = Fixture::new();
        let (persistent_handler, persistence_tracker) = track(TestPersistenceHandler {});
        fixture.add(persistent_handler, |h| {
            h.handles::<TestSucceeded>();
        });

        let (output_handler, output_tracker) = track(TestOutputDisplayHandler {});
        fixture.add(output_handler, |h| {
            h.handles::<TestSucceeded>();
        });

        assert_eq!(fixture.start().is_ok(), true);

        let dispatched = fixture
            .dispatch_remote(&TestSucceeded {
                execution_time_secs: 0.5,
            })
            .await
            .expect("failed to dispatch event");

        assert!(dispatched.descriptor.is::<TestSucceeded>());
        assert_eq!(persistence_tracker.count_for::<TestSucceeded>(), 1);
        assert_eq!(output_tracker.count_for::<TestSucceeded>(), 1);
    }
}
