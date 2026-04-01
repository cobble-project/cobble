//! JNI bridge crate for Cobble Java binding.
//!
//! This crate is internal to Java binding packaging and exports JNI symbols
//! consumed by `cobble-java/java` classes.
//! End users should use the Java APIs under `io.cobble.*` instead of calling
//! these Rust modules directly.
//!
#![allow(dead_code)]

mod coordinator;
mod db;
mod read_only_db;
mod read_options;
mod reader;
mod scan;
mod schema;
mod single_db;
mod structured;
mod structured_single_db;
mod util;
mod write_options;
