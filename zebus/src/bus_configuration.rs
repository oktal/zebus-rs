use std::time::Duration;

pub const DEFAULT_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_START_REPLAY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Configuration parameters for a [`Bus`]
#[derive(Debug, Clone)]
pub struct BusConfiguration {
    /// The list of directories that can be used by the Bus to register.
    /// The syntax is `tcp://hostname:port`
    pub directory_endpoints: Vec<String>,

    /// The time to for when registering to a Directory, once this time is over,
    /// the next directory in the list will be used.
    pub registration_timeout: Duration,

    /// The time to wait for when trying to replay messages from the persistence on startup.
    /// Failing to get a response from the Persistence in the allotted time causes the Peer to stop.
    pub start_replay_timeout: Duration,

    /// A peer marked as persistent will benefit from the [`persistence`] mechanism
    /// persistence: https://github.com/Abc-Arbitrage/Zebus/wiki/Persistence
    pub is_persistent: bool,

    /// Mainly a debugging setting, setting it to false will prevent the Bus from connecting
    /// to a random Directory when needed
    pub pick_random_directory: bool,

    /// Indicates whether [`MessageProcessingFailed`] should be published on handler errors.
    pub enable_error_publication: bool,

    /// Maximum batch size for [`BatchedMessageHandler`]
    pub message_batch_size: usize,
}
