use std::{
    io,
    sync::mpsc,
    thread::{self, JoinHandle},
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// IO Error
    #[error("IO {0}")]
    Io(io::Error),

    /// An error occured when attempting to send a message to the queue
    #[error("error sending message to dispatch queue")]
    SendError,

    /// An operation was attempted while the queue was not in a valid state
    #[error("An operation was attempted while the dispatcher was not in a valid state")]
    InvalidOperation,
}

enum Inner {
    Init {
        name: String,
    },

    Started {
        name: String,
        tx: mpsc::Sender<super::Task>,
        handle: JoinHandle<()>,
    },
}

struct Worker {
    rx: mpsc::Receiver<super::Task>,
}

impl Worker {
    fn start(name: &str) -> Result<(std::sync::mpsc::Sender<super::Task>, JoinHandle<()>), Error> {
        // Create channel for message dispatching
        let (tx, rx) = mpsc::channel();

        // Create tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::Io)?;

        // Create Worker
        let mut worker = Worker { rx };

        // Spawn worker thread
        let thread_name = format!("dispatch-{name}");
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                rt.block_on(async move { worker.run().await });
            })
            .map_err(Error::Io)?;

        Ok((tx, handle))
    }

    async fn run(&mut self) {
        while let Ok(task) = self.rx.recv() {
            task.invoke().await;
        }
    }
}

pub(super) struct DispatchQueue {
    inner: Option<Inner>,
}

impl DispatchQueue {
    /// Create a new named `name` dispatch queue
    pub(super) fn new(name: String) -> Self {
        Self {
            inner: Some(Inner::Init { name }),
        }
    }

    /// Get the name of this dispatch queue
    pub(super) fn name(&self) -> Result<&str, Error> {
        match self.inner.as_ref() {
            Some(Inner::Init { name, .. }) | Some(Inner::Started { name, .. }) => Ok(&name),
            _ => Err(Error::InvalidOperation),
        }
    }

    pub(super) fn start(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init { name }) => {
                // Start worker
                let (tx, handle) = Worker::start(&name)?;

                // Transition to Started state
                (Some(Inner::Started { name, tx, handle }), Ok(()))
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    pub(super) fn spawn(&self, task: super::Task) -> Result<(), Error> {
        match self.inner {
            Some(Inner::Started { ref tx, .. }) => tx.send(task).map_err(|_e| Error::SendError),
            _ => Err(Error::InvalidOperation),
        }
    }
}
