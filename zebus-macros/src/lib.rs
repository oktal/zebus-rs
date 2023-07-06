//! Crate that contains expansion logic for zebus macros.
//!
//! # [`Command`] and [`Event`] derive macros
//!
//! For a given definition of a message:
//!
//! ```
//! use zebus_macros::Command;
//!
//! #[derive(Command)]
//! #[zebus(namespace = "Abc.Namespace", routable)]
//! struct MyCommand {
//!     pub name: String,
//!
//!     #[zebus(routing_position = 1)]
//!     pub segment: u32,
//!
//!     #[zebus(routing_position = 2)]
//!     pub id: u64
//! }
//! ```
//! [`Command`] and [`Event`] derive proc macros will expand to the following:
//!
//! ```ignore
//! struct MyCommandBinding {
//!     pub segment: Binding<u32>,
//!
//!     pub id: Binding<u64>
//! }
//!
//! impl MessageBinding for MyCommand {
//!      type Binding = MyCommandBinding;
//!
//!      fn bind(binding: Self::Binding) -> BindingKey {
//!          BindingKey {
//!              fragments: vec![
//!                  binding.segment.bind(),
//!                  binding.id.bind()
//!              ]
//!          }
//!      }
//! }
//! ```
//!
//! ```ignore
//! impl Message for MyCommand {
//!     fn name() -> &'static str {
//!         "Abc.Namespace.MyCommand"
//!     }
//!
//!     fn routing() -> &'static [RoutingField] {
//!         &[
//!             RoutingField {
//!                 index: 0,
//!                 routing_position: 1
//!             },
//!             RoutingField {
//!                 index: 1,
//!                 routing_position: 2,
//!             }
//!        ]
//!    }
//!
//!    fn get_binding(&self) -> BindingKey {
//!        vec![
//!            self.segment.to_string(),
//!            self.id.to_string()
//!        ].into()
//!    }
//! }
//! ```
//!
//! [`Command`] macro will also implement the [`zebus_core::Command`] trait while [`Event`] will
//! implement [`zebus_core::Event`] trait
//!
//! # [`Handler`] derive macro
//!
//! This macro will implement the [`zebus_core::DispatchHandler`] trait:
//!
//! ```
//! use zebus_macros::Handler;
//!
//! #[derive(Handler)]
//! #[zebus(dispatch_queue = "CustomQueue")]
//! struct MyHandler {}
//! ```
//!
//! Which will expand to the following:
//!
//! ```
//! use zebus_core::DispatchHandler;
//! struct MyHandler {}
//!
//! impl DispatchHandler for MyHandler {
//!     const DISPATCH_QUEUE: &'static str = "CustomQueue";
//! }
//! ```
//!
//! # [`handler`] attribute macro
//!
//! This macro can be applied on `impl Handler<T> for Handler` impl blocks to specify the
//! subscription policy at startup for a given message type, and other options for the handler.
//!
//! This macro will implement the [`zebus_core::HandlerDescriptor`] trait.
//!

use proc_macro::TokenStream;

mod attribute;
pub(crate) mod attrs;
mod derive;

#[proc_macro_derive(Command, attributes(zebus))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    derive::command(input).unwrap_or_else(|e| e.into_compile_error().into())
}

#[proc_macro_derive(Event, attributes(zebus))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    derive::event(input).unwrap_or_else(|e| e.into_compile_error().into())
}

#[proc_macro_derive(Handler, attributes(zebus))]
pub fn derive_handler(input: TokenStream) -> TokenStream {
    derive::handler(input).unwrap_or_else(|e| e.into_compile_error().into())
}

#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    attribute::handler(attr, item)
}
