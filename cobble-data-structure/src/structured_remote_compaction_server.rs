use cobble::{Config, RemoteCompactionServer, Result};
use std::net::TcpStream;
use std::sync::Arc;

use crate::structured_db::{
    structured_merge_operator_resolver, structured_resolvable_operator_ids,
};

/// A remote compaction server pre-configured to resolve structured data type
/// merge operators (e.g. list) from request metadata.
///
/// This wraps [`RemoteCompactionServer`] and automatically registers the
/// structured merge operator resolver on construction.
pub struct StructuredRemoteCompactionServer {
    inner: Arc<RemoteCompactionServer>,
}

impl StructuredRemoteCompactionServer {
    pub fn new(config: Config) -> Result<Self> {
        let server = RemoteCompactionServer::new(config)?;
        server.set_merge_operator_resolver(
            structured_merge_operator_resolver(),
            structured_resolvable_operator_ids(),
        );
        Ok(Self {
            inner: Arc::new(server),
        })
    }

    pub fn supported_merge_operator_ids(&self) -> Vec<String> {
        self.inner.supported_merge_operator_ids()
    }

    pub fn serve(&self, address: &str) -> Result<()> {
        self.inner.serve(address)
    }

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        self.inner.handle_connection(stream)
    }

    pub fn inner(&self) -> &RemoteCompactionServer {
        &self.inner
    }

    pub fn close(&self) {
        self.inner.close()
    }
}

#[cfg(test)]
#[path = "../tests/unit/structured_remote_compaction_server.rs"]
mod tests;
