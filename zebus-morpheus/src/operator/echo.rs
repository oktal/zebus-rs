use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use zebus::dispatch::{RouteHandler, Router};
use zebus::{Bus, Command, ResponseMessage};

use crate::opts::Opts;

#[derive(Command, prost::Message, Clone)]
#[zebus(namespace = "Zebus.Morpheus.Simulation.Echo")]
struct EchoCommand {
    #[prost(string, tag = 1)]
    text: String,
}

#[derive(prost::Message, Clone, Command)]
#[zebus(namespace = "Zebus.Morpheus.Simulation.Echo")]
struct EchoResponse {
    #[prost(string, tag = 1)]
    text: String,
}

impl From<EchoCommand> for EchoResponse {
    fn from(value: EchoCommand) -> Self {
        Self { text: value.text }
    }
}

async fn handle_echo(cmd: EchoCommand) -> ResponseMessage<EchoResponse> {
    EchoResponse::from(cmd).into()
}

pub(crate) async fn start(opts: Opts, shutdown: CancellationToken) -> anyhow::Result<()> {
    info!("starting echo operator");

    let bus = opts
        .create_bus(
            "Zebus.Morpheus.Echo.Operator",
            None,
            Router::with_state(()).handles(handle_echo.into_handler()),
        )
        .await?;

    if let Err(e) = bus.start().await {
        error!("{e}");
    }
    shutdown.cancelled().await;
    bus.stop().await?;

    Ok(())
}
