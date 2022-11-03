use std::collections::HashMap;

use thiserror::Error;

use super::{queue::DispatchQueue, registry::Registry, Dispatcher, MessageDispatch};
use crate::{transport::TransportMessage, DispatchHandler};

/// Errors that can be returned by the [`MessageDispatcher`]
#[derive(Debug, Error)]
pub enum Error {
    /// An operation on the [`DispatchQueue`] returned an eror
    #[error("dispatch queue error {0}")]
    Queue(super::queue::Error),

    /// Attempted to register a handler for a message to a different dispatch queue
    #[error("attempted to register message {message_type} to a different dispatch queue {dispatch_queue}, was previously registered to {previous}")]
    DoubleRegister {
        message_type: &'static str,
        dispatch_queue: &'static str,
        previous: &'static str,
    },

    /// An operation was attempted while the [`MessageDispatcher`] was in an invalid state for the
    /// operation
    #[error("an operation was attempted while the dispatcher was not in a valid state")]
    InvalidOperation,
}

/// Inner state of the dispatcher
enum Inner {
    Init {
        dispatch_queues: HashMap<&'static str, DispatchQueue>,
        message_dispatch_queue: HashMap<&'static str, &'static str>,
    },

    Started {
        dispatch_queues: HashMap<&'static str, DispatchQueue>,
        message_dispatch_queue: HashMap<&'static str, &'static str>,
    },
}

pub(crate) struct MessageDispatcher {
    inner: Option<Inner>,
}

impl MessageDispatcher {
    /// Create a new empty dispatcher
    pub(crate) fn new() -> Self {
        Self {
            inner: Some(Inner::Init {
                dispatch_queues: HashMap::new(),
                message_dispatch_queue: HashMap::new(),
            }),
        }
    }

    /// Add a [`Registry`] of handlers to the dispatcher
    pub(crate) fn add<H>(&mut self, registry: Box<Registry<H>>) -> Result<(), Error>
    where
        H: DispatchHandler + Send + 'static,
    {
        match self.inner.as_mut() {
            Some(Inner::Init {
                ref mut dispatch_queues,
                ref mut message_dispatch_queue,
            }) => {
                // Retrieve the dispatch queue for this handler
                let dispatch_queue = H::DISPATCH_QUEUE;

                // Register all handled messages to the dispatch queue and make sure the user did
                // not attempt to register a message handler to a different dispatch queue
                for handled_message in registry.handled_messages() {
                    match message_dispatch_queue.entry(handled_message) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            Err(Error::DoubleRegister {
                                message_type: handled_message,
                                dispatch_queue,
                                previous: e.key(),
                            })
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(dispatch_queue);
                            Ok(())
                        }
                    }?;
                }

                // Create the dispatch queue if it does not exist and add the registry to the
                // dispatch queue
                dispatch_queues
                    .entry(H::DISPATCH_QUEUE)
                    .or_insert(DispatchQueue::new(dispatch_queue.to_string()))
                    .add(registry)
                    .map_err(Error::Queue)?;

                Ok(())
            }
            _ => Err(Error::InvalidOperation),
        }
    }

    /// Start the [`MessageDispatcher`]. This will start all the registered [`DispatchQueue`]
    /// queues
    pub(crate) fn start(&mut self) -> Result<(), Error> {
        let (inner, res) = match self.inner.take() {
            Some(Inner::Init {
                mut dispatch_queues,
                message_dispatch_queue,
            }) => {
                // Start all the dispatch queues
                for (_, queue) in &mut dispatch_queues {
                    queue.start().map_err(Error::Queue)?;
                }

                // Transition to Started state
                (
                    Some(Inner::Started {
                        dispatch_queues,
                        message_dispatch_queue,
                    }),
                    Ok(()),
                )
            }
            x => (x, Err(Error::InvalidOperation)),
        };
        self.inner = inner;
        res
    }

    /// Attempt to retrieve the [`DispatchQueue`] associated with the [`TransportMessage`] message
    /// Returns an error if the operation was invalid.
    /// Returns `Some` with a mutable reference of the queue if the queue was found or `None` otherwise.
    fn get_queue_mut(
        &mut self,
        message: &TransportMessage,
    ) -> Result<Option<&mut DispatchQueue>, Error> {
        let (dispatch_queues, message_dispatch_queues) = match self.inner.as_mut() {
            Some(Inner::Init {
                dispatch_queues,
                message_dispatch_queue,
            })
            | Some(Inner::Started {
                dispatch_queues,
                message_dispatch_queue,
            }) => Ok((dispatch_queues, message_dispatch_queue)),
            _ => Err(Error::InvalidOperation),
        }?;

        Ok(message_dispatch_queues
            .get_mut(message.message_type_id.full_name.as_str())
            .and_then(|dispatch_queue| dispatch_queues.get_mut(dispatch_queue)))
    }
}

impl Dispatcher for MessageDispatcher {
    fn dispatch(&mut self, message: TransportMessage) {
        if let Ok(Some(dispatch_queue)) = self.get_queue_mut(&message) {
            // TODO(oktal): correctly handle underlying result
            dispatch_queue
                .send(MessageDispatch::for_message(message))
                .unwrap();
        }
    }
}
