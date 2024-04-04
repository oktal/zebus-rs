use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use zebus::dispatch::{InvokerHandler, MessageHandler};
use zebus::{Bus, Command, ResponseMessage};

use crate::opts::Opts;

const NAME: &'static str = "echo";

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
    info!("starting {NAME} operator");

    let bus = opts
        .create_bus(
            "Zebus.Morpheus.Echo.Operator",
            None,
            MessageHandler::with_state(()).handles(handle_echo.into_handler()),
        )
        .await?;

    if let Err(e) = bus.start().await {
        error!("{e}");
    }
    shutdown.cancelled().await;
    bus.stop().await?;

    Ok(())
}
