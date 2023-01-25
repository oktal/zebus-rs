//! Simple trait for helping along upcasting of dyn supertraits
//! from https://docs.rs/upcast/latest/upcast/index.html
//!
//! ```
//! # use zebus_core::{Upcast, UpcastFrom};
//! pub trait A {}
//! pub trait B: A + Upcast<dyn A> {}
//!
//! // Put this in your library
//! impl<'a, T: A + 'a> UpcastFrom<T> for dyn A + 'a {
//!     fn up_from(value: &T) -> &(dyn A + 'a) { value }
//!     fn up_from_mut(value: &mut T) -> &mut (dyn A + 'a) { value }
//! }
//!
//! // Now your users can do an upcast if needed, or you can within implementations
//! fn do_cast(b: &dyn B) -> &dyn A {
//!     b.up()
//! }
//! ```

/// Implement this trait for your `dyn Trait` types for all `T: Trait`
pub trait UpcastFrom<T: ?Sized> {
    fn up_from(value: &T) -> &Self;
    fn up_from_mut(value: &mut T) -> &mut Self;
}

/// Use this trait to perform your upcasts on dyn traits. Make sure to require it in the supertrait!
pub trait Upcast<U: ?Sized> {
    fn up(&self) -> &U;
    fn up_mut(&mut self) -> &mut U;
}

impl<T: ?Sized, U: ?Sized> Upcast<U> for T
where
    U: UpcastFrom<T>,
{
    fn up(&self) -> &U {
        U::up_from(self)
    }
    fn up_mut(&mut self) -> &mut U {
        U::up_from_mut(self)
    }
}
