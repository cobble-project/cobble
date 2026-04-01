//! Embeddable web monitor backend for Cobble.
//!
//! This crate provides an HTTP service that reads via `cobble::Reader` and exposes:
//! - health endpoint,
//! - metadata endpoint,
//! - inspect endpoints for lookup/scan style debugging.
//!
//! Main entry points:
//! - `MonitorConfig`
//! - `MonitorServer`
//! - `MonitorServerHandle`
//!
#![crate_type = "lib"]

mod error;
mod server;

pub use error::{Error, Result};
pub use server::{
    InspectItem, InspectMode, InspectQuery, MetaResponse, MonitorConfig, MonitorConfigSource,
    MonitorServer, MonitorServerHandle,
};
