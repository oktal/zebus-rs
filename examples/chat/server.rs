//! This example demonstrates how to write a simple peer to peer chat communication through zebus commands and events
//! This is the server side of our simple chat application.

use messages::{MessageSent, RegisterUserCommand, SendMessageCommand, UserConnected};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::{error, info, warn};
use zebus::configuration::DefaultConfigurationProvider;
use zebus::directory::{PeerDecommissioned, PeerStopped};
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::inject::State;
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{inject, Bus, BusBuilder, BusConfiguration, ConfigurationProvider, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::messages::UserDisconnected;
use crate::stop::StopSignal;

mod error;
mod messages;
mod stop;

#[derive(Debug, Error)]
enum RegisterError {
    #[error("User name that was provided already exists")]
    UserAlreadyExists,
}

impl zebus::Error for RegisterError {
    fn code(&self) -> i32 {
        match self {
            Self::UserAlreadyExists => error::CHAT_ERROR_USER_ALREADY_EXISTS,
        }
    }
}

struct UserEntry {
    name: String,
}

struct SessionState {
    users: HashMap<PeerId, Arc<UserEntry>>,
}

type AppSessionState = Arc<Mutex<SessionState>>;

async fn register_user(
    cmd: RegisterUserCommand,
    inject::Bus(bus): inject::Bus,
    inject::Originator(originator): inject::Originator,
    State(state): inject::State<AppSessionState>,
) -> Result<(), RegisterError> {
    info!(
        "peer {} registered with username {}",
        originator.sender_id, cmd.username
    );

    let inserted = {
        let mut state = state.lock().expect("lock poisoned");
        state
            .users
            .insert(
                originator.sender_id,
                Arc::new(UserEntry {
                    name: cmd.username.clone(),
                }),
            )
            .is_none()
    };

    if !inserted {
        return Err(RegisterError::UserAlreadyExists);
    }

    if let Err(e) = bus
        .publish(&UserConnected {
            username: cmd.username,
        })
        .await
    {
        error!("failed to publish UserRegistered event: {e}");
    }

    Ok(())
}

async fn send_message(
    cmd: SendMessageCommand,
    inject::Bus(bus): inject::Bus,
    inject::Originator(originator): inject::Originator,
    State(state): inject::State<AppSessionState>,
) {
    let entry = {
        let state = state.lock().expect("lock poisoned");
        state.users.get(&originator.sender_id).cloned()
    };

    match entry {
        Some(entry) => {
            let _ = bus
                .publish(&MessageSent {
                    target: cmd.target,
                    sender: entry.name.clone(),
                    text: cmd.text,
                })
                .await;
        }
        None => {
            warn!(
                "unknown peer {} attempted to send a message",
                originator.sender_id
            );
        }
    }
}

async fn disconnect_peer(peer: &PeerId, state: AppSessionState, bus: Arc<dyn Bus>) {
    let entry = {
        let mut state = state.lock().expect("lock poisoned");
        state.users.remove(&peer)
    };

    if let Some(entry) = entry {
        info!("{} disconnected", entry.name);

        if let Err(e) = bus
            .publish(&UserDisconnected {
                username: entry.name.clone(),
            })
            .await
        {
            warn!("failed to publish UserDisconnected: {e}");
        }
    }
}

async fn peer_decommisionned(
    ev: PeerDecommissioned,
    inject::Bus(bus): inject::Bus,
    State(state): inject::State<AppSessionState>,
) {
    disconnect_peer(&ev.id, state, bus).await;
}

async fn peer_stopped(
    ev: PeerStopped,
    inject::Bus(bus): inject::Bus,
    State(state): inject::State<AppSessionState>,
) {
    disconnect_peer(&ev.id, state, bus).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/zmq.toml")
        .configure()?;

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut configuration =
        DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/bus.toml");

    let state = Arc::new(Mutex::new(SessionState {
        users: HashMap::new(),
    }));

    let bus = BusBuilder::new()
        .configure_with(PeerId::new("Chat.Server.0"), "example", &mut configuration)?
        .with_handler(
            MessageHandler::with_state(state)
                .handles(register_user.into_handler())
                .handles(send_message.into_handler())
                .handles(peer_stopped.into_handler())
                .handles(peer_decommisionned.into_handler()),
        )
        .with_transport(zmq)
        .create()
        .await?;

    if let Err(e) = bus.start().await {
        error!("failed to start bus: {e}");
    } else {
        let stop = StopSignal::new()?;
        println!("Waiting for Ctrl-C to exit...");
        stop.await;
        bus.stop().await.expect("Failed to stop bus");
    }

    Ok(())
}
