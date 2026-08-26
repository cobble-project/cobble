//! Structured APIs used by language bridge crates.

pub use cobble_data_structure::*;

#[doc(hidden)]
pub mod ffi {
    pub use cobble_data_structure::ffi::*;
}
