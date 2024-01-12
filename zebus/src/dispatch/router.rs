use std::{
    any::{type_name, Any},
    collections::{hash_map::Entry, HashMap},
    convert::Infallible,
    marker::PhantomData,
    task::Poll,
};

use futures_core::{future::BoxFuture, Future};
use futures_util::FutureExt;
use pin_project::pin_project;
use tower::{util::BoxService, ServiceExt};
use tower_service::Service;
use zebus_core::{HandlerDescriptor, MessageTypeDescriptor};

use crate::{
    inject::{self, Extract},
    BindingKey, IntoResponse, Message, MessageDescriptor, Response, SubscriptionMode,
};

use super::{
    invoker::{InvokeRequest, InvokerService},
    MessageInvokerDescriptor,
};

#[pin_project(project = ErrorOrResponseProj)]
pub enum ErrorOrResponse<R, F>
where
    R: IntoResponse,
    F: Future<Output = R>,
{
    Error(Option<Response>),
    Response(#[pin] F),
}

impl<R, F> Future for ErrorOrResponse<R, F>
where
    R: IntoResponse,
    F: Future<Output = R>,
{
    type Output = Option<Response>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project() {
            ErrorOrResponseProj::Error(e) => Poll::Ready(e.take()),
            ErrorOrResponseProj::Response(fut) => match fut.poll(cx) {
                Poll::Ready(resp) => Poll::Ready(resp.into_response()),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

pub trait IntoHandler<S> {
    type Service;

    fn into_handler(self, state: S) -> (Box<Self::Service>, MessageInvokerDescriptor);
}

impl<S, H> IntoHandler<S> for H
where
    H: HandlerDescriptor<S>,
    H::Service: Service<InvokeRequest>,
    <<H as HandlerDescriptor<S>>::Service as Service<InvokeRequest>>::Future: Send,
    H::Binding: Into<BindingKey>,
    S: Clone + Send + 'static,
{
    type Service = H::Service;

    fn into_handler(self, state: S) -> (Box<Self::Service>, MessageInvokerDescriptor) {
        let descriptor = MessageInvokerDescriptor {
            invoker_type: self.name(),
            dispatch_queue: self.queue(),
            message: self.message(),
            subscription_mode: self.subscription_mode(),
            bindings: self.bindings().into_iter().map(Into::into).collect(),
        };
        let service = self.service(state);
        (Box::new(service), descriptor)
    }
}

pub trait RouteHandlerDescriptor<Args> {
    /// Type of message handled by the handler
    fn message(&self) -> MessageTypeDescriptor;

    /// Name of the handler. The name will usually correspond to the type name of the handler
    fn name(&self) -> &'static str;
}

pub struct MakeRouteHandler<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>>,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
{
    handler: H,
    descriptor: MessageInvokerDescriptor,
    phantom: PhantomData<fn(Args, S) -> Fut>,
}

impl<Args, S, H, Fut> MakeRouteHandler<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>> + Send + 'static,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
{
    fn new(handler: H) -> Self {
        Self {
            handler,
            descriptor: MessageInvokerDescriptor {
                invoker_type: handler.name(),
                dispatch_queue: None,
                message: handler.message(),
                subscription_mode: SubscriptionMode::Auto,
                bindings: vec![],
            },
            phantom: PhantomData,
        }
    }

    pub fn in_dispatch_queue(mut self, name: &'static str) -> Self {
        self.descriptor.dispatch_queue = Some(name);
        self
    }

    pub fn subscription_mode(mut self, mode: SubscriptionMode) -> Self {
        self.descriptor.subscription_mode = mode;
        self
    }

    pub fn with_bindings<B, I>(mut self, bindings: B) -> Self
    where
        B: IntoIterator<Item = I>,
        I: Into<BindingKey>,
    {
        self.descriptor
            .bindings
            .extend(bindings.into_iter().map(Into::into));
        self
    }
}

impl<Args, S, H, Fut> HandlerDescriptor<S> for MakeRouteHandler<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>> + Send + 'static,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
    Args: 'static,
{
    type Service = RouteHandlerService<Args, S, H, Fut>;
    type Binding = BindingKey;

    fn service(self, state: S) -> Self::Service {
        self.handler.into_service(state)
    }

    fn message(&self) -> MessageTypeDescriptor {
        self.handler.message()
    }

    fn queue(&self) -> Option<&'static str> {
        self.descriptor.dispatch_queue
    }

    fn name(&self) -> &'static str {
        self.handler.name()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        self.descriptor.subscription_mode
    }

    fn bindings(&self) -> Vec<Self::Binding> {
        self.descriptor.bindings.clone()
    }
}

pub trait RouteHandler<Args, S>:
    Copy + Clone + Send + Sized + Any + RouteHandlerDescriptor<Args> + 'static
where
    S: Clone + Send + 'static,
{
    type Future: Future<Output = Option<Response>> + Send + 'static;

    fn handle(self, req: InvokeRequest, state: S) -> Self::Future;

    fn into_service(self, state: S) -> RouteHandlerService<Args, S, Self, Self::Future> {
        RouteHandlerService::new(self, state)
    }

    fn into_handler(self) -> MakeRouteHandler<Args, S, Self, Self::Future> {
        MakeRouteHandler::new(self)
    }
}

pub struct RouteHandlerService<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>>,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
{
    handler: H,
    state: S,
    phantom: PhantomData<fn(Args) -> Fut>,
}

impl<Args, S, H, Fut> RouteHandlerService<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>>,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
{
    fn new(handler: H, state: S) -> Self {
        Self {
            handler,
            state,
            phantom: PhantomData,
        }
    }
}

impl<Args, S, H, Fut> Service<InvokeRequest> for RouteHandlerService<Args, S, H, Fut>
where
    Fut: Future<Output = Option<Response>>,
    H: RouteHandler<Args, S, Future = Fut>,
    S: Clone + Send + 'static,
{
    type Response = Option<Response>;
    type Error = Infallible;

    type Future = futures_util::future::Map<
        Fut,
        fn(Option<Response>) -> Result<Option<Response>, Infallible>,
    >;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: InvokeRequest) -> Self::Future {
        let handler = self.handler.clone();
        let future = RouteHandler::handle(handler, req, self.state.clone());
        let future = future.map(Ok as _);
        future
    }
}

macro_rules! impl_tuple {
    ($($arg:ident $t:tt),*) => {
        impl<F, Fut, M, $($t,)* S, R> RouteHandler<(M, $($t,)*), S> for F
        where
            F: FnOnce(M, $($t,)*) -> Fut + Copy + Clone + Send + 'static,
            Fut: Future<Output = R> + Send + 'static,
            M: Message + MessageDescriptor + prost::Message + Clone + Default,
            $($t: Extract<S> + 'static,)*
            S: Clone + Send + 'static,
            R: IntoResponse + 'static,
        {
            type Future = ErrorOrResponse<R, Fut>;

            fn handle(self, req: InvokeRequest, state: S) -> Self::Future {
                let dispatch = &req.dispatch;
                $(
                    let $arg = match $t::extract(&dispatch, &state) {
                        Ok(arg) => arg,
                        Err(e) => return ErrorOrResponse::Error(e.into_response()),
                    };

                )*
                let msg = inject::message::Message::extract(&dispatch, &state).map(|m| m.0);

                match msg {
                    Ok(msg) => ErrorOrResponse::Response(self(msg, $($arg,)*)),
                    Err(e) => ErrorOrResponse::Error(e.into_response()),
                }
            }
        }

        impl<F, Fut, M, $($t,)* R> RouteHandlerDescriptor<(M, $($t,)*)> for F
        where
            F: FnOnce(M, $($t,)*) -> Fut + Copy + Clone + Send + 'static,
            Fut: Future<Output = R> + Send + 'static,
            M: Message + MessageDescriptor + prost::Message + Clone + Default,
            R: IntoResponse + 'static,
        {
            fn message(&self) -> MessageTypeDescriptor {
                MessageTypeDescriptor::of::<M>()
            }

            fn name(&self) -> &'static str {
                type_name::<Self>()
            }
        }
    };
}

impl_tuple!();
impl_tuple!(arg0 A);
impl_tuple!(arg0 A, arg1 B);
impl_tuple!(arg0 A, arg1 B, arg2 C);
impl_tuple!(arg0 A, arg1 B, arg2 C, arg3 D);
impl_tuple!(arg0 A, arg1 B, arg2 C, arg3 D, arg4 E);
impl_tuple!(arg0 A, arg1 B, arg2 C, arg3 D, arg4 E, arg5 G);

pub struct Router<S>
where
    S: Clone + Send + 'static,
{
    invokers: HashMap<
        &'static str,
        (
            MessageInvokerDescriptor,
            BoxService<InvokeRequest, Option<Response>, Infallible>,
        ),
    >,
    state: S,
}

impl<S> Router<S>
where
    S: Clone + Send + 'static,
{
    pub fn with_state(state: S) -> Self {
        Self {
            invokers: HashMap::new(),
            state,
        }
    }
}

impl<S> Router<S>
where
    S: Clone + Send + 'static,
{
    pub fn handles<H>(mut self, handler: H) -> Self
    where
        H: IntoHandler<S>,
        <H as IntoHandler<S>>::Service: Service<InvokeRequest, Response = Option<Response>, Error = Infallible>
            + Send
            + 'static,
        <<H as IntoHandler<S>>::Service as Service<InvokeRequest>>::Future: Send,
    {
        let (service, descriptor) = handler.into_handler(self.state.clone());
        let message_type = descriptor.message.full_name;

        match self.invokers.entry(message_type) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => Some(e.insert((descriptor.clone(), service.boxed()))),
        }
        .expect(&format!(
            "attempted to double-insert handler for {}",
            message_type
        ));
        self
    }

    #[cfg(test)]
    async fn route(&mut self, req: InvokeRequest) -> Option<Response> {
        if let Some((_, service)) = self.invokers.get_mut(req.dispatch.message().message_type()) {
            service.call(req).await.expect("invokers are infaillible")
        } else {
            None
        }
    }
}

impl<S> Service<InvokeRequest> for Router<S>
where
    S: Clone + Send + 'static,
{
    type Response = Option<Response>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Option<Response>, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: InvokeRequest) -> Self::Future {
        self.invokers
            .get_mut(req.dispatch.message().message_type())
            .expect("call should only be called after poll_invoke returns `Some`")
            .1
            .call(req)
    }
}

impl<S> InvokerService for Router<S>
where
    S: Clone + Send + 'static,
{
    fn descriptors(&self) -> Vec<MessageInvokerDescriptor> {
        self.invokers.values().map(|i| i.0.clone()).collect()
    }

    fn poll_invoke<'a>(
        &'a self,
        req: &InvokeRequest,
    ) -> Poll<Option<&'a MessageInvokerDescriptor>> {
        Poll::Ready(
            self.invokers
                .get(req.dispatch.message().message_type())
                .map(|i| &i.0),
        )
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::{
        bus, core::MessagePayload, dispatch::DispatchRequest, inject::state::State,
        transport::TransportMessage, Bus, Command, Event, Peer, PeerId, ResponseMessage,
    };

    use super::*;

    struct Fixture<S>
    where
        S: Clone + Send + 'static,
    {
        router: Router<S>,
        peer: Peer,
        environment: String,
    }

    struct TestBus;

    impl<S> Fixture<S>
    where
        S: Clone + Send + 'static,
    {
        fn new<F>(state: S, route_fn: F) -> Self
        where
            F: FnOnce(Router<S>) -> Router<S>,
        {
            let router = Router::with_state(state);
            Self {
                router: route_fn(router),
                peer: Peer::test(),
                environment: "Test".to_string(),
            }
        }

        async fn route<M>(&mut self, msg: M) -> Option<Response>
        where
            M: Message + MessageDescriptor + prost::Message + Clone + Default,
        {
            self.route_with(msg, |message, bus| DispatchRequest::remote(message, bus))
                .await
        }

        async fn route_with<M, F>(&mut self, msg: M, req_fn: F) -> Option<Response>
        where
            M: Message + MessageDescriptor + prost::Message + Clone + Default,
            F: FnOnce(TransportMessage, Arc<dyn Bus>) -> DispatchRequest,
        {
            self.router.route(self.request_for(msg, req_fn)).await
        }

        fn request_for<M, F>(&self, msg: M, req_fn: F) -> InvokeRequest
        where
            M: Message + MessageDescriptor + prost::Message + Clone + Default,
            F: FnOnce(TransportMessage, Arc<dyn Bus>) -> DispatchRequest,
        {
            let (_, message) = TransportMessage::create(&self.peer, self.environment.clone(), &msg);
            InvokeRequest {
                dispatch: Arc::new(req_fn(message, Arc::new(TestBus))),
            }
        }
    }

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

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Coffee")]
    struct BrewCommand {
        #[prost(fixed64, tag = 1)]
        grams_in: u64,

        #[prost(fixed64, tag = 2)]
        grams_out: u64,

        #[prost(fixed32, tag = 3)]
        pressure: u32,
    }

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Coffee")]
    struct GrindCommand {
        #[prost(fixed32, tag = 1)]
        bur_spread: u32,

        #[prost(fixed32, tag = 2)]
        grams_out: u32,
    }

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Coffee")]
    struct GrindResponse {
        #[prost(fixed32, tag = 1)]
        remaining: u32,
    }

    #[derive(prost::Message, crate::Command, Clone, Eq, PartialEq)]
    #[zebus(namespace = "Abc.Coffee")]
    struct HeatSteamBoilerCommand {
        #[prost(fixed32, tag = 1)]
        temperature: u32,
    }

    fn decode_as<M>(response: Option<Response>) -> Option<M>
    where
        M: MessageDescriptor + prost::Message + Default,
    {
        response.and_then(|response| {
            if let Response::Message(message) = response {
                message.decode_as::<M>().and_then(|r| r.ok())
            } else {
                None
            }
        })
    }

    async fn brew(_cmd: BrewCommand) {}

    #[derive(Debug, Copy, Clone)]
    struct Grinder {
        grams_capacity: u32,
    }

    impl Grinder {
        fn with_capacity(grams: u32) -> Self {
            Self {
                grams_capacity: grams,
            }
        }
    }

    async fn grind(
        cmd: GrindCommand,
        State(grinder): State<Grinder>,
    ) -> ResponseMessage<GrindResponse> {
        GrindResponse {
            remaining: grinder
                .grams_capacity
                .checked_sub(cmd.grams_out)
                .unwrap_or(0),
        }
        .into()
    }

    #[tokio::test]
    async fn route_message_simple() {
        let mut fixture = Fixture::new((), |r| r.handles(brew.into_handler()));

        let response = fixture
            .route(BrewCommand {
                grams_in: 17,
                grams_out: 40,
                pressure: 9,
            })
            .await;

        assert!(response.is_none());
    }

    #[tokio::test]
    async fn route_message_with_state_injection() {
        let mut fixture = Fixture::new(Grinder::with_capacity(100), |r| {
            r.handles(grind.into_handler())
        });

        let response = fixture
            .route(GrindCommand {
                grams_out: 20,
                bur_spread: 45,
            })
            .await;

        assert_eq!(
            decode_as::<GrindResponse>(response),
            Some(GrindResponse { remaining: 80 })
        );
    }

    #[tokio::test]
    async fn route_message_with_custom_dispatch() {
        let fixture = Fixture::new((), |r| {
            r.handles(brew.into_handler().in_dispatch_queue("Brewer"))
        });

        let descriptor = fixture.router.poll_invoke(&fixture.request_for(
            BrewCommand {
                grams_in: 17,
                grams_out: 40,
                pressure: 9,
            },
            |message, bus| DispatchRequest::remote(message, bus),
        ));

        let dispatch_queue = match descriptor {
            Poll::Ready(Some(descriptor)) => descriptor.dispatch_queue,
            _ => None,
        };

        assert_eq!(dispatch_queue, Some("Brewer"));
    }
}