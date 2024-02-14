use std::collections::HashMap;
use std::task::Poll;

use thiserror::Error;
use tower_service::Service;
use zebus_core::DEFAULT_DISPATCH_QUEUE;

use super::future::DispatchFuture;
use super::invoker::{InvokeRequest, InvokerService};
use super::queue::DispatchQueue;
use super::{DispatchRequest, DispatchService, Dispatched, MessageInvokerDescriptor};
use crate::core::MessagePayload;
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

    /// List of services that can invoke handlers
    services: Vec<Box<dyn InvokerService>>,
}

impl InvokerDispatcher {
    fn new() -> Self {
        Self {
            dispatch_queues: HashMap::new(),
            descriptors: HashMap::new(),
            services: Vec::new(),
        }
    }

    fn add(&mut self, invoker: Box<dyn InvokerService>) -> Result<(), Error> {
        for descriptor in invoker.descriptors() {
            let dispatch_queue = descriptor.dispatch_queue.unwrap_or(DEFAULT_DISPATCH_QUEUE);

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

        self.services.push(invoker);
        Ok(())
    }

    fn start(&mut self) -> Result<(), Error> {
        for (_name, dipatch_queue) in &mut self.dispatch_queues {
            dipatch_queue.start().map_err(Error::Queue)?;
        }

        Ok(())
    }

    fn stop(&mut self) -> Result<(), Error> {
        for (_name, dipatch_queue) in &mut self.dispatch_queues {
            dipatch_queue.stop().map_err(Error::Queue)?;
        }

        Ok(())
    }

    fn dispatch(&mut self, request: DispatchRequest) -> DispatchFuture {
        // Retrieve the descriptor for the message we are about to dispatch and
        // return an error'd future if we failed to find it
        let message_type = request
            .message
            .message_type()
            .expect("A dispatch request should have a message type");

        let descriptor = match self.descriptors.get(message_type) {
            Some(descriptor) => descriptor,
            None => return super::future::err(Error::UnknownMessage(message_type.to_string())),
        };

        // Create the dispatch context
        let (context, rx) = super::context::new(request);

        // Create the invoke request
        let invoke_request = InvokeRequest {
            dispatch: context.request(),
        };

        // Iterate through the services and create the tasks to invoke
        // the handlers for the message of the given request
        let tasks = self.services.iter_mut().filter_map(|s| {
            if let Some((message, dispatch_queue, invoker_type)) =
                match s.poll_invoke(&invoke_request) {
                    Poll::Ready(Some(descriptor)) => Some((
                        descriptor.message,
                        descriptor.dispatch_queue.unwrap_or(DEFAULT_DISPATCH_QUEUE),
                        descriptor.invoker_type,
                    )),
                    _ => None,
                }
            {
                let future = s.call(invoke_request.clone());
                Some(super::Task {
                    ctx: context.clone(),
                    future,
                    message,
                    dispatch_queue,
                    handler_type: invoker_type,
                })
            } else {
                None
            }
        });

        let mut total_invokers = 0;

        for task in tasks {
            // For an infrastructure message, spawn the task and start the execution of the invoker right away
            if task.message.is_infrastructure {
                tokio::spawn(task.invoke());
            } else {
                // Retrieve the dispatch queue for the message
                let queue = self
                    .dispatch_queues
                    .get(task.dispatch_queue)
                    .expect("unconfigured dispatch queue");

                // Attempt to spawn the task in the dispatch queue and return an error'd future if we failed
                if let Err(e) = queue.spawn(task) {
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
    pub(crate) fn add(&mut self, invoker: Box<dyn InvokerService>) -> Result<(), Error> {
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

    /// Stop the [`MessageDispatcher`]. This will stop all the registered [`DispatchQueue`]
    /// queues
    pub(crate) fn stop(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Started(mut dispatch)) => {
                // Stop all the dispatchers
                dispatch.stop()?;

                // Transition to Init state
                (Some(Inner::Init(dispatch)), Ok(()))
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
    fn descriptors(&self) -> Result<Vec<MessageInvokerDescriptor>, self::Error> {
        self.descriptors().map(|it| it.cloned().collect())
    }
}

impl Service<DispatchRequest> for MessageDispatcher {
    type Response = Dispatched;
    type Error = Error;
    type Future = DispatchFuture;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DispatchRequest) -> Self::Future {
        self.dispatch(req)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{
        bus::{self, CommandResult},
        core::MessagePayload,
        dispatch::{
            router::{RouteHandler, RouteHandlerDescriptor, Router},
            DispatchRequest, DispatchResult, Dispatched,
        },
        handler,
        inject::{self, State},
        transport::TransportMessage,
        Bus, Command, Event, HandlerDescriptor, HandlerError, Message, MessageDescriptor, Peer,
        PeerId, Response, ResponseMessage, SubscriptionMode,
    };

    struct TestBus;

    #[async_trait]
    impl Bus for TestBus {
        async fn configure(&self, _peer_id: PeerId, _environment: String) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn start(&self) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn stop(&self) -> bus::Result<()> {
            Err(bus::Error::InvalidOperation)
        }

        async fn send(&self, _command: &dyn Command) -> CommandResult {
            Err(bus::Error::InvalidOperation.into())
        }

        async fn send_to(&self, _command: &dyn Command, _peer: Peer) -> CommandResult {
            Err(bus::Error::InvalidOperation.into())
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

        fn add<S, F>(&mut self, state: S, route_fn: F)
        where
            S: Clone + Send + 'static,
            F: FnOnce(Router<S>) -> Router<S>,
        {
            let router = route_fn(Router::with_state(state));
            self.dispatcher
                .add(Box::new(router))
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

    /// Component to track handler invocation
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

    #[derive(prost::Message, crate::Command, Clone)]
    #[zebus(namespace = "Abc.Test")]
    struct TestCommand {}

    #[derive(prost::Message, crate::Event, Clone)]
    #[zebus(namespace = "Abc.Test")]
    struct TestSucceeded {
        #[prost(double, required, tag = 1)]
        execution_time_secs: f64,
    }

    async fn parse_noop(cmd: ParseCommand, inject::State(tracker): State<Tracker>) {
        tracker.track(&cmd);
    }

    async fn parse(
        cmd: ParseCommand,
    ) -> Result<ResponseMessage<ParseResponse>, HandlerError<ParseError>> {
        let mut chars = cmd.value.chars();
        if let Some(first) = chars.next() {
            if first == '-' {
                return Err(HandlerError::User(ParseError::Negative));
            }
        }

        Ok(ParseResponse {
            value: cmd.value.parse::<i64>()?,
        }
        .into())
    }

    async fn ping(
        cmd: PingCommand,
        inject::State(tracker): State<Tracker>,
    ) -> ResponseMessage<PongResponse> {
        tracker.track(&cmd);
        PongResponse { seq: cmd.seq }.into()
    }

    async fn test_succeded(msg: TestSucceeded, inject::State(tracker): State<Tracker>) {
        tracker.track(&msg);
    }

    async fn display_test_succeeded(msg: TestSucceeded, inject::State(tracker): State<Tracker>) {
        tracker.track(&msg);
    }

    #[test]
    fn generate_handler_with_subscription_mode() {
        #[handler(auto)]
        async fn test_auto(_cmd: TestCommand) {}

        #[handler(manual)]
        async fn test_manual(_cmd: TestCommand) {}

        assert_eq!(
            <test_auto as HandlerDescriptor<()>>::subscription_mode(&test_auto),
            SubscriptionMode::Auto
        );

        assert_eq!(
            <test_manual as HandlerDescriptor<()>>::subscription_mode(&test_manual),
            SubscriptionMode::Manual
        );
    }

    #[test]
    fn generate_handler_with_dispatch_queue() {
        #[handler(auto)]
        async fn test(_cmd: TestCommand) {}

        #[handler(auto, queue = "TestQueue")]
        async fn test_queue(_cmd: TestCommand) {}

        assert_eq!(<test as HandlerDescriptor<()>>::queue(&test), None);
        assert_eq!(
            <test_queue as HandlerDescriptor<()>>::queue(&test_queue),
            Some("TestQueue")
        );
    }

    #[tokio::test]
    async fn dispatch_message_to_handler_with_no_response() {
        let mut fixture = Fixture::new();
        let tracker = Tracker::new();
        fixture.add(tracker.clone(), |router| {
            router.handles(parse_noop.into_handler())
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
        fixture.add((), |router| router.handles(parse.into_handler()));

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
    #[allow(unused_variables)]
    async fn dispatch_message_to_handler_with_user_error() {
        let mut fixture = Fixture::new();
        fixture.add((), |router| router.handles(parse.into_handler()));

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
        fixture.add((), |router| router.handles(parse.into_handler()));

        assert_eq!(fixture.start().is_ok(), true);

        let command = ParseCommand {
            value: "NotANumber".to_string(),
        };
        let dispatched = fixture
            .dispatch_remote(&command)
            .await
            .expect("failed to dispatch command");

        let err = dispatched.result.unwrap_err();

        assert!(!err.is_empty());
        assert!(dispatched.descriptor.is::<ParseCommand>());
    }

    #[tokio::test]
    async fn dispatch_local_message_to_handler_with_no_response() {
        let mut fixture = Fixture::new();
        let tracker = Tracker::new();
        fixture.add(tracker.clone(), |router| {
            router.handles(parse_noop.into_handler())
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
        fixture.add((), |router| router.handles(parse.into_handler()));

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
        let tracker = Tracker::new();
        fixture.add(tracker.clone(), |router| {
            router.handles(parse_noop.into_handler())
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
        fixture.add((), |router| router.handles(parse.into_handler()));

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
        fixture.add((), |router| router.handles(parse.into_handler()));

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
        let tracker = Tracker::new();
        fixture.add(tracker.clone(), |router| {
            router.handles(ping.into_handler())
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
        let tracker = Tracker::new();
        fixture.add(tracker.clone(), |router| {
            router.handles(
                test_succeded
                    .into_handler()
                    .in_dispatch_queue("TestPersistenceQueue"),
            )
        });
        fixture.add(tracker.clone(), |router| {
            router.handles(
                display_test_succeeded
                    .into_handler()
                    .in_dispatch_queue("TestDisplayQueue"),
            )
        });

        assert_eq!(fixture.start().is_ok(), true);

        let dispatched = fixture
            .dispatch_remote(&TestSucceeded {
                execution_time_secs: 0.5,
            })
            .await
            .expect("failed to dispatch event");

        assert!(dispatched.descriptor.is::<TestSucceeded>());
        assert_eq!(tracker.count_for::<TestSucceeded>(), 2);
    }
}
