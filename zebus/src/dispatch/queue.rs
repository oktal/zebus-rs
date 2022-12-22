use std::{
    io,
    sync::mpsc,
    thread::{self, JoinHandle},
};

use super::{Dispatch, MessageDispatch};
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
        dispatchers: Vec<Box<dyn Dispatch + Send>>,
    },

    Started {
        name: String,
        dispatch_tx: mpsc::Sender<MessageDispatch>,
        handle: JoinHandle<()>,
    },
}

struct Worker {
    dispatch_rx: mpsc::Receiver<MessageDispatch>,
    dispatchers: Vec<Box<dyn Dispatch + Send>>,
}

impl Worker {
    fn start(
        name: &str,
        dispatchers: Vec<Box<dyn Dispatch + Send>>,
    ) -> Result<(std::sync::mpsc::Sender<MessageDispatch>, JoinHandle<()>), Error> {
        // Create channel for message dispatching
        let (tx, rx) = mpsc::channel();

        // Create worker
        let mut worker = Worker {
            dispatch_rx: rx,
            dispatchers,
        };

        // Spawn worker thread
        let thread_name = format!("dispatch-{name}");
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                // TODO(oktal): Bubble up the error to the JoinHandle
                worker.run();
            })
            .map_err(Error::Io)?;

        Ok((tx, handle))
    }

    fn run(&mut self) {
        while let Ok(dispatch) = self.dispatch_rx.recv() {
            self.dispatchers.dispatch(&dispatch);
            dispatch.set_completed();
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
            inner: Some(Inner::Init {
                name,
                dispatchers: vec![],
            }),
        }
    }

    /// Get the name of this dispatch queue
    pub(super) fn name(&self) -> Result<&str, Error> {
        match self.inner.as_ref() {
            Some(Inner::Init { name, .. }) | Some(Inner::Started { name, .. }) => Ok(&name),
            _ => Err(Error::InvalidOperation),
        }
    }

    pub(super) fn add(&mut self, dispatcher: Box<dyn Dispatch + Send>) -> Result<(), Error> {
        match self.inner.as_mut() {
            Some(Inner::Init {
                ref mut dispatchers,
                ..
            }) => {
                dispatchers.push(dispatcher);
                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }

    pub(super) fn start(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init { name, dispatchers }) => {
                // Start worker
                let (dispatch_tx, handle) = Worker::start(&name, dispatchers)?;

                // Transition to Started state
                (
                    Some(Inner::Started {
                        name,
                        dispatch_tx,
                        handle,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };

        self.inner = inner;
        res
    }

    pub(super) fn send(&mut self, dispatch: MessageDispatch) -> Result<(), Error> {
        match self.inner {
            Some(Inner::Started {
                ref dispatch_tx, ..
            }) => dispatch_tx.send(dispatch).map_err(|_e| Error::SendError),
            _ => Err(Error::InvalidOperation),
        }
    }
}
