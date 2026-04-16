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
mod tests {
    use super::*;
    use crate::StructuredColumnValue;
    use crate::list::{ListConfig, ListRetainMode};
    use cobble::VolumeDescriptor;
    use size::Size;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_structured_remote_compaction_server_supported_ids() {
        let root = format!("/tmp/ds_remote_ids_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            compaction_threads: 1,
            ..Config::default()
        };
        let server = StructuredRemoteCompactionServer::new(config).unwrap();
        let ids = server.supported_merge_operator_ids();
        assert!(
            ids.contains(&"cobble.list.v1".to_string()),
            "should include cobble.list.v1, got: {:?}",
            ids
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_structured_remote_compaction_with_list_operator() {
        let root = format!("/tmp/ds_remote_list_{}", Uuid::new_v4());

        // Set up the remote compaction server
        let server_config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            base_file_size: Size::from_const(128),
            compaction_threads: 1,
            num_columns: 1,
            ..Config::default()
        };
        let server = Arc::new(StructuredRemoteCompactionServer::new(server_config).unwrap());

        // Start server in a background thread using serve()
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let server_clone = Arc::clone(&server);
        let addr_str = addr.to_string();
        let _server_thread = thread::spawn(move || {
            let _ = server_clone.serve(&addr_str);
        });
        // Give the server a moment to bind
        thread::sleep(Duration::from_millis(100));

        // Open a StructuredSingleDb with remote compaction pointing to our server
        let db_config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            total_buckets: 1,
            base_file_size: Size::from_const(128),
            compaction_threads: 1,
            l0_file_limit: 2,
            compaction_remote_addr: Some(addr.to_string()),
            ..Config::default()
        };
        let mut db = crate::StructuredSingleDb::open(db_config).unwrap();
        db.update_schema()
            .add_list_column(
                None,
                1,
                ListConfig {
                    max_elements: Some(5),
                    retain_mode: ListRetainMode::Last,
                    preserve_element_ttl: false,
                },
            )
            .commit()
            .unwrap();

        // Write enough data to trigger flush and eventually compaction
        for i in 0..30u32 {
            let key = format!("key{:03}", i % 5);
            db.put(
                0,
                key.as_bytes(),
                0,
                bytes::Bytes::from(format!("val{}", i)),
            )
            .unwrap();
            db.merge(
                0,
                key.as_bytes(),
                1,
                vec![bytes::Bytes::from(format!("elem{}", i))],
            )
            .unwrap();
        }

        // Trigger a snapshot to ensure data is flushed
        let _snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(500));

        // Read back and verify list merge results are correct
        for i in 0..5u32 {
            let key = format!("key{:03}", i);
            let row = db.get(0, key.as_bytes()).unwrap();
            assert!(row.is_some(), "key {} should exist", key);
            let row = row.unwrap();
            // Column 1 is a list — elements should be present
            if let Some(StructuredColumnValue::List(elements)) = &row[1] {
                assert!(
                    !elements.is_empty(),
                    "key {} list column should have elements",
                    key
                );
            } else {
                panic!("key {} column 1 should be a list, got {:?}", key, row[1]);
            }
        }

        db.close().unwrap();
        server.close();
        let _ = std::fs::remove_dir_all(&root);
    }
}
