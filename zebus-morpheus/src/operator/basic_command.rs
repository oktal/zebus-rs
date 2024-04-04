use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use zebus::{
    dispatch::{InvokerHandler, MessageHandler},
    inject, Bus, Command,
};

use crate::opts::Opts;

use super::messages::SimulationStarted;

const NAME: &'static str = "basic-command";

#[derive(prost::Message, Command, Clone)]
#[zebus(namespace = "Zebus.Morpheus.Simulation.BasicCommand")]
struct BasicCommand {
    #[prost(fixed64, tag = 1)]
    seq: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct BasicCommandParameters {
    count: u64,

    seq: u64,
}

#[derive(Clone)]
struct SimulationState {
    events_tx: mpsc::Sender<SimulationStarted>,
}

async fn handle_simulation_started(
    evt: SimulationStarted,
    inject::State(state): inject::State<SimulationState>,
) {
    state
        .events_tx
        .send(evt)
        .await
        .expect("publish event failed");
}

pub(crate) async fn start(opts: Opts, _shutdown: CancellationToken) -> anyhow::Result<()> {
    info!("starting {NAME} operator");

    let (events_tx, events_rx) = mpsc::channel(128);
    let state = SimulationState { events_tx };

    let events_rx = tokio_stream::wrappers::ReceiverStream::new(events_rx);

    let bus = opts
        .create_bus(
            "Zebus.Morpheus.BasicCommand.Operator",
            None,
            MessageHandler::with_state(state).handles(handle_simulation_started.into_handler()),
        )
        .await?;

    bus.start().await?;

    let mut start = events_rx.filter(|ev| ev.name.eq_ignore_ascii_case(NAME));

    loop {
        debug!("waiting for started event...");
        let Some(started) = start.next().await else {
            break;
        };

        let params: BasicCommandParameters = serde_json::from_str(&started.params)?;
        info!("{NAME} started with {params:?} parameters");

        let seq_start = params.seq;
        let seq_end = params.seq + params.count;

        for seq in seq_start..seq_end {
            let cmd = BasicCommand { seq };

            if let Err(e) = bus.send(&cmd).await {
                error!("error sending command {e}");
            }
        }
    }

    Ok(())
}
