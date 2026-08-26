//! Shared import surface for Cobble language bindings.
//!
//! This crate intentionally contains no database facade or transport layer. It
//! only groups the Rust APIs used by the language-specific JNI and CXX crates.

pub use cobble::*;

#[doc(hidden)]
pub mod ffi;

pub mod structured;
