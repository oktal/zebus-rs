use std::time::Duration;

use rand::{seq::SliceRandom, thread_rng};

use crate::{Peer, PeerId};

/// Default time to wait for when registering to a Directory
pub const DEFAULT_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Default time to wait for when trying to replay messages from the persistence on startup
pub const DEFAULT_START_REPLAY_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum batch size for [`crate::BatchedMessageHandler`]
pub const DEFAULT_MAX_BATCH_SIZE: usize = 100;

/// Configuration parameters for a [`Bus`](crate::Bus)
#[derive(Debug, Clone)]
pub struct BusConfiguration {
    /// The list of directories that can be used by the Bus to register.
    /// The syntax is `tcp://hostname:port`
    pub directory_endpoints: Vec<String>,

    /// The time to wait for when registering to a Directory, once this time is over,
    /// the next directory in the list will be used.
    pub registration_timeout: Duration,

    /// The time to wait for when trying to replay messages from the persistence on startup.
    /// Failing to get a response from the Persistence in the allocated time causes the Peer to stop.
    pub start_replay_timeout: Duration,

    /// A peer marked as persistent will benefit from the [persistence](https://github.com/Abc-Arbitrage/Zebus/wiki/Persistence) mechanism
    pub is_persistent: bool,

    /// Mainly a debugging setting, setting it to false will prevent the Bus from connecting
    /// to a random Directory when needed
    pub pick_random_directory: bool,

    /// Indicates whether [MessageProcessingFailed](crate::lotus::MessageProcessingFailed) should be published on handler errors.
    pub enable_error_publication: bool,

    /// Maximum batch size for [`crate::BatchedMessageHandler`]
    pub message_batch_size: usize,
}

impl BusConfiguration {
    /// Update the configuration with the given directory `endpoints`
    pub fn with_directory_endpoints<T: Into<String>>(
        mut self,
        endpoints: impl IntoIterator<Item = T>,
    ) -> Self {
        self.directory_endpoints
            .extend(endpoints.into_iter().map(Into::into));
        self
    }

    /// Update the configuration with the given registration `timeout`
    pub fn with_registration_timeout(mut self, timeout: Duration) -> Self {
        self.registration_timeout = timeout;
        self
    }

    /// Update the configuration with the given start replay `timeout`
    pub fn with_start_replay_timeout(mut self, timeout: Duration) -> Self {
        self.start_replay_timeout = timeout;
        self
    }

    /// Update the configuration to toggle random directory selection on registration
    pub fn with_random_directory(mut self, value: bool) -> Self {
        self.pick_random_directory = value;
        self
    }

    /// Update the configuration to toggle persistence
    /// Enabling persistence will make the peer persistent and benefit from the
    /// [persistence](https://github.com/Abc-Arbitrage/Zebus/wiki/Persistence) mechanism
    pub fn with_persistence(mut self, value: bool) -> Self {
        self.is_persistent = value;
        self
    }

    /// Update the configuration to toggle
    /// [MessageProcessingFailed](crate::lotus::MessageProcessingFailed) publishing on handler errors
    pub fn with_error_publication(mut self, value: bool) -> Self {
        self.enable_error_publication = value;
        self
    }

    /// Return an iterator of [`Peer`] peers over the configured directory endpoints
    /// Shuffle the peers if [`Self::pick_random_directory`] is true
    pub(crate) fn directory_peers(&self) -> Vec<Peer> {
        let mut peers = self
            .directory_endpoints
            .iter()
            .enumerate()
            .map(|(idx, endpoint)| {
                let peer_id = PeerId::directory(idx);

                Peer {
                    id: peer_id,
                    endpoint: endpoint.to_string(),
                    is_up: true,
                    is_responding: true,
                }
            })
            .collect::<Vec<_>>();

        if self.pick_random_directory {
            peers.shuffle(&mut thread_rng());
        }

        peers
    }
}

impl Default for BusConfiguration {
    fn default() -> Self {
        Self {
            directory_endpoints: Vec::new(),
            registration_timeout: DEFAULT_REGISTRATION_TIMEOUT,
            start_replay_timeout: DEFAULT_START_REPLAY_TIMEOUT,
            is_persistent: false,
            pick_random_directory: true,
            enable_error_publication: false,
            message_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }
}
