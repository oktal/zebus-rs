use zebus::{Command, Event};

/// Command sent by the client to register to the server with a specified user name
#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "SimpleChat")]
pub struct RegisterUserCommand {
    #[prost(string, tag = 1)]
    pub username: String,
}

/// Command sent by the client to send a message to a target channel
#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "SimpleChat")]
pub struct SendMessageCommand {
    #[prost(string, tag = 1)]
    pub target: String,

    #[prost(string, tag = 2)]
    pub text: String,
}

/// Event published by the server to notify other peers that a client has connected
#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "SimpleChat")]
pub struct UserConnected {
    #[prost(string, tag = 1)]
    pub username: String,
}

/// Event published by the server to notify other peers that a client has disconnected
#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "SimpleChat")]
pub struct UserDisconnected {
    #[prost(string, tag = 1)]
    pub username: String,
}

/// Event published by the server to notify other peers that a message has been sent by a client
#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "SimpleChat", routable)]
pub struct MessageSent {
    #[prost(string, tag = 1)]
    #[zebus(routing_position = 1)]
    pub target: String,

    #[prost(string, tag = 2)]
    pub sender: String,

    #[prost(string, tag = 3)]
    pub text: String,
}
