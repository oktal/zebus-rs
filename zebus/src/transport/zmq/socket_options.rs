use std::time::Duration;

pub const DEFAULT_SEND_HIGH_WATERMARK: i32 = 20000;
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_millis(200);
pub const DEFAULT_RECV_HIGH_WATERMARK: i32 = 40000;
pub const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_millis(300);

pub const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(3);

/// Keep alive options that can be set to a zmq [`Socket`]
#[derive(Copy, Clone, Debug)]
pub struct KeepAliveOptions {
    /// Set the `ZMQ_TCP_KEEPALIVE` option to the provided value
    /// Override SO_KEEPALIVE socket option
    pub timeout: Option<Duration>,

    /// Set the `ZMQ_TCP_KEEPALIVE_INTVL` option to the provided value
    /// Override TCP_KEEPINTVL socket option
    pub interval: Option<Duration>,
}

/// Options that can be set to a zmq [`Socket`]
/// Please refer to [`zmq_setsockopt`]: http://api.zeromq.org/3-0:zmq-setsockopt for a complete
/// documentation of all available options
#[derive(Copy, Clone, Debug)]
pub struct ZmqSocketOptions {
    /// Set the `ZMQ_SNDHWM` option to the provided value
    /// Set high water mark for outbound messages
    pub send_high_watermark: Option<i32>,

    /// Set the `ZMQ_RCVHWM` option to the provided value
    /// Set high water mark for inbound messages
    pub recv_high_watermark: Option<i32>,

    /// Set the `ZMQ_SNDTIMEO` option to the provided value
    /// Maximum time before a send operation returns with EAGAIN
    pub send_timeout: Option<Duration>,

    /// Set the `ZMQ_RCVTIMEO` option to the provided value
    /// Maximum time before a recv operation returns with EAGAIN
    pub recv_timeout: Option<Duration>,

    /// Set [`KeepAliveOptions`] zmq socket options
    pub keep_alive: Option<KeepAliveOptions>,
}

impl Default for ZmqSocketOptions {
    fn default() -> Self {
        Self {
            send_high_watermark: Some(DEFAULT_SEND_HIGH_WATERMARK),
            recv_high_watermark: Some(DEFAULT_RECV_HIGH_WATERMARK),
            send_timeout: Some(DEFAULT_SEND_TIMEOUT),
            recv_timeout: Some(DEFAULT_RECV_TIMEOUT),
            keep_alive: Some(KeepAliveOptions {
                timeout: Some(DEFAULT_KEEP_ALIVE_TIMEOUT),
                interval: Some(DEFAULT_KEEP_ALIVE_INTERVAL),
            }),
        }
    }
}
