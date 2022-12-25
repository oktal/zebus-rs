use std::{
    io,
    os::unix::io::{AsRawFd, RawFd},
    task::{Context, Poll},
};

use tokio::io::{unix::AsyncFd, ReadBuf};

/// Asynchronous wrapper around zmq file descriptor
struct ZmqAsyncFd {
    socket: zmq::Socket,
    fd: RawFd,
}

impl ZmqAsyncFd {
    fn new(socket: zmq::Socket) -> Result<Self, zmq::Error> {
        Ok(Self {
            fd: socket.get_fd()?,
            socket,
        })
    }
}

impl AsRawFd for ZmqAsyncFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

pub(super) struct ZmqPoller(AsyncFd<ZmqAsyncFd>);

impl ZmqPoller {
    pub(super) fn new(socket: zmq::Socket) -> Result<Self, io::Error> {
        Ok(Self(AsyncFd::new(ZmqAsyncFd::new(socket)?)?))
    }

    pub(super) fn recv(&self, buf: &mut [u8], flags: i32) -> Result<usize, zmq::Error> {
        let socket = self.socket();
        socket.recv_into(buf, flags)
    }

    pub(super) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Get the socket
        let socket = self.socket();

        // Get the pending events on the socket
        let events = socket.get_events()?;

        // Make sure we can read from the socket
        if !events.contains(zmq::POLLIN) {
            // This one is a little bit weird. It looks like we can be woken up
            // by a "read-ready" event from the runtime on the underlying file
            // descriptor, while the zmq event queue is empty.
            // If that's the case, make sure to clear the ready flag and wake
            // the task up so that we keep getting polled
            if let Poll::Ready(mut guard) = self.0.poll_read_ready(cx)? {
                guard.clear_ready();
                cx.waker().wake_by_ref()
            }
            return Poll::Pending;
        }

        // Number of remaining bytes available for read in the buffer
        let remaining = buf.remaining();

        // Get a reference to the unitialized part of the read buffer we can read into
        let b =
            unsafe { &mut *(buf.unfilled_mut() as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]) };

        // Read from the socket
        match socket.recv_into(b, 0) {
            Ok(n) => {
                // Safety: we trust zmq to have initialized our buffer
                unsafe {
                    buf.assume_init(n);
                }
                buf.advance(n);

                if n <= remaining {
                    // We have read all the bytes of the message
                    Poll::Ready(Ok(()))
                } else {
                    // We sill have bytes to read, make sure we keep being polled by the underlying
                    // runtime
                    Poll::Pending
                }
            }
            Err(zmq::Error::EAGAIN) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e.into())),
        }
    }

    fn socket(&self) -> &zmq::Socket {
        &self.0.get_ref().socket
    }
}
