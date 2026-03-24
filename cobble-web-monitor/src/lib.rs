#![crate_type = "lib"]

mod error;
mod server;

pub use error::{Error, Result};
pub use server::{
    InspectItem, InspectMode, InspectQuery, MetaResponse, MonitorConfig, MonitorConfigSource,
    MonitorServer, MonitorServerHandle,
};
