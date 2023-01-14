//! Provides a type with [`std::cell::Cell`] like semantic to access data with either exclusive
//! or shared access wrapped through a [`Mutex`]
//!
//! This type can be used to access data through exclusive or shared access, withouth paying
//! the cost of locking the underlying value everytime if data will be accessed with exclusive
//! access
//!
//! This mechanism is well-suited for types that can optionally be sent to threads
//!
//! # Example
//!
//! ```ignore
//!
//! struct CallbackInfo {
//!     synchronous: bool,
//!     token: usize
//! }
//!
//! fn worker(info: LockCell<CallbackInfo>) {
//! }
//!
//! fn start() {
//!     let callbacks = vec![
//!         CallbackInfo { synchronous: true, token: 0 },
//!         CallbackInfo { synchronous: false, token: 1 },
//!         CallbackInfo { synchronous: false, token: 2 }
//!     ];
//!
//!     for callback in callbacks {
//!         let synchronous = callback.synchronous;
//!         let cb = LockCell::new(callback);
//!
//!         if !synchronous {
//!             let [cb0, cb1] = cb.into_shared::<2>();
//!             std::thread::spawn(|| {
//!                 worker(cb0);
//!             });
//!
//!             // Do something with cb1 which is safe to use accross threads
//!         } else {
//!             // cb can be used with exclusive access without a lock
//!         }
//!     }
//! }
//! ```
use std::sync::{Arc, Mutex};

pub(crate) enum LockCell<T> {
    /// Exclusive access to the underlying value
    Exclusive(T),

    /// Shared value wrapped inside a [`Mutex`]
    Shared(Arc<Mutex<T>>),
}

impl<T> LockCell<T> {
    /// Create a new exclusive [`LockCell`]
    pub(crate) fn new(value: T) -> Self {
        Self::Exclusive(value)
    }

    /// Consumes `self`, make it [`Self::Shared`] if it was [`Self::Exclusive`] before,
    /// and returns [`N`] [`self::Shared`] instances
    pub(crate) fn into_shared<const N: usize>(self) -> [Self; N] {
        let shared = match self {
            Self::Exclusive(value) => Arc::new(Mutex::new(value)),
            Self::Shared(shared) => shared,
        };

        std::array::from_fn(|_| Self::Shared(Arc::clone(&shared)))
    }

    /// Apply an [`op`] operation to the underlying value
    pub(crate) fn apply<R>(&self, op: impl FnOnce(&T) -> R) -> R {
        match self {
            Self::Exclusive(value) => op(value),
            Self::Shared(mtx) => {
                let value = mtx.lock().unwrap();
                op(&*value)
            }
        }
    }

    /// Apply an [`op`] operation mutably to the underlying value
    pub(crate) fn apply_mut<R>(&mut self, op: impl FnOnce(&mut T) -> R) -> R {
        match self {
            Self::Exclusive(ref mut value) => op(value),
            Self::Shared(mtx) => {
                let mut value = mtx.lock().unwrap();
                op(&mut *value)
            }
        }
    }
}

impl<T: Clone> Clone for LockCell<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Exclusive(value) => Self::Exclusive(value.clone()),
            Self::Shared(mtx) => Self::Shared(Arc::clone(mtx)),
        }
    }
}
