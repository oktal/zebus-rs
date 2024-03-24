use std::io::{self, Read};

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use clap::Parser;
use opts::Opts;
use tokio_util::sync::CancellationToken;

mod operator;
mod opts;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("ZEBUS_LOG"))
        .init();

    let opts = Opts::parse();

    let shutdown = CancellationToken::new();

    let mut handles = Vec::new();

    handles.push(tokio::spawn(operator::echo::start(
        opts.clone(),
        shutdown.clone(),
    )));

    println!("Press a key to exit");

    io::stdin()
        .bytes()
        .next()
        .expect("Failed to read input")
        .unwrap();

    shutdown.cancel();

    Ok(())
}
