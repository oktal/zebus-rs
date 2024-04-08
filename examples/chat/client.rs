//! This example demonstrates how to write a simple peer to peer chat communication through zebus commands and events
//! This is the client side of our simple chat application.
//! To register a client to a server, start the client with a username and a list of channels
//! ```
//! cargo run --example chat-client krab news general
//! ```
//!
//! The client supports the following commands:
//! * `/exit`: exits the client and stops the bus
//! * `/default [channel]` set the default channel to `channel`. All messages will be sent to this channel
//! * `/send [channel] [msg]` send the `msg` to the target `channel`
//!
//! To send a message to a previously configured default channel, simply type your message and press ENTER

use std::io::{self, Write};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use messages::{MessageSent, RegisterUserCommand, UserConnected, UserDisconnected};
use tracing::{error, info};
use zebus::configuration::DefaultConfigurationProvider;
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::transport::zmq::{ZmqSocketOptions, ZmqTransport, ZmqTransportConfiguration};
use zebus::{inject, Bus, BusBuilder, BusConfiguration, ConfigurationProvider, PeerId};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::messages::SendMessageCommand;

mod error;
mod messages;

enum Command {
    Exit,
    Default { target: String },
    Send { target: String, text: String },
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("exit") {
            return Ok(Self::Exit);
        }

        let (cmd, args) = s.split_once(' ').ok_or(anyhow!("invalid command {s}"))?;

        if cmd.eq_ignore_ascii_case("exit") {
            Ok(Self::Exit)
        } else if cmd.eq_ignore_ascii_case("default") {
            Ok(Self::Default {
                target: args.to_owned(),
            })
        } else if cmd.eq_ignore_ascii_case("send") {
            let (target, text) = args
                .split_once(' ')
                .ok_or(anyhow!("command /send target text"))?;
            Ok(Self::Send {
                target: target.to_owned(),
                text: text.to_owned(),
            })
        } else {
            Err(anyhow!("unknown command {cmd}"))
        }
    }
}

struct ClientState {
    username: String,
}

async fn user_connected(ev: UserConnected, inject::State(state): inject::State<Arc<ClientState>>) {
    if state.username == ev.username {
        return;
    }

    println!("{} joined the server !", ev.username);
}

async fn user_disconnected(ev: UserDisconnected) {
    println!("{} left the server !", ev.username);
}

async fn message_sent(ev: MessageSent, inject::State(state): inject::State<Arc<ClientState>>) {
    if state.username == ev.sender {
        return;
    }

    println!("[#{}] {} > {}", ev.target, ev.sender, ev.text);
}

async fn run(bus: &dyn Bus) -> anyhow::Result<()> {
    let mut text = String::new();
    let mut exit = false;

    let mut default = None;

    while !exit {
        io::stdout().flush().expect("flush stdout");

        if io::stdin().read_line(&mut text).is_err() {
            break;
        }

        let msg = text.trim_end();
        if msg.chars().next() == Some('/') {
            let cmd = &msg[1..];
            match cmd.parse::<Command>() {
                Ok(cmd) => match cmd {
                    Command::Exit => exit = true,
                    Command::Send { target, text } => {
                        println!("> {msg}");
                        bus.send(&SendMessageCommand { text, target })
                            .await
                            .context("send message")?;
                    }
                    Command::Default { target } => {
                        println!("Set default channel to {target}");
                        default = Some(target);
                    }
                },
                Err(e) => eprintln!("{e}"),
            }
        } else {
            if let Some(default) = default.as_ref() {
                println!("> {msg}");
                bus.send(&SendMessageCommand {
                    text: msg.to_owned(),
                    target: default.to_owned(),
                })
                .await
                .context("send message")?;
            } else {
                eprintln!("No default channel. Set a default channel with /default [channel]");
            }
        }

        text.clear();
    }

    Ok(())
}

struct Opts {
    username: String,
    channels: Vec<String>,
}

impl Opts {
    fn parse() -> anyhow::Result<Self> {
        let mut args = std::env::args().skip(1);
        let Some(username) = args.next() else {
            bail!("Usage client [username] [channel1] [channel2] [...]");
        };

        let channels = args.collect::<Vec<_>>();
        Ok(Opts { username, channels })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    let opts = Opts::parse()?;
    if opts.channels.is_empty() {
        bail!("Usage client [username] [channel1] [channel2] [...]");
    }

    let username = &opts.username;

    let zmq_configuration = DefaultConfigurationProvider::<ZmqTransportConfiguration>::default()
        .with_file("examples/zmq.toml")
        .configure()?;

    let zmq_socket_opts = ZmqSocketOptions::default();
    let zmq = ZmqTransport::new(zmq_configuration, zmq_socket_opts);

    let mut configuration =
        DefaultConfigurationProvider::<BusConfiguration>::default().with_file("examples/bus.toml");

    let state = Arc::new(ClientState {
        username: opts.username.clone(),
    });

    let bus = BusBuilder::new()
        .configure_with(
            PeerId::new(format!("Chat.Client.{username}")),
            "example",
            &mut configuration,
        )?
        .with_handler(
            MessageHandler::with_state(state)
                .handles(user_connected.into_handler())
                .handles(user_disconnected.into_handler())
                .handles(
                    message_sent
                        .into_handler()
                        .bind_all(opts.channels.clone(), |binding, channel| {
                            binding.target.matches(channel)
                        }),
                ),
        )
        .with_transport(zmq)
        .create()
        .await
        .context("create bus")?;

    bus.start().await.context("start bus")?;

    info!("registering with username {username} ...");

    bus.send(&RegisterUserCommand {
        username: opts.username.clone(),
    })
    .await
    .context("register")?;

    info!("... registered");

    let joined_channels = opts
        .channels
        .iter()
        .map(|c| {
            if c.starts_with('#') {
                c.clone()
            } else {
                format!("#{c}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    println!("joined {joined_channels}");

    if let Err(e) = run(&bus).await {
        error!("{e}");
    }

    bus.stop().await.context("stop bus")?;

    Ok(())
}
