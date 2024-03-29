use zebus::Event;

#[derive(prost::Message, Event, Clone)]
#[zebus(namespace = "Zebus.Morpheus.Simulation")]
pub(super) struct SimulationStarted {
    #[prost(string, tag = 1)]
    pub name: String,

    #[prost(string, tag = 2)]
    pub params: String,
}
