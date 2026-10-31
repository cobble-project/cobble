use crate::Result;
use crate::file::{FileSystem, RandomAccessFile, SequentialWriteFile};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use url::Url;

/// A delete-only backend whose completion is controlled by the test, not by sleeps.
struct DeleteProbe {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    result: Result<()>,
}

pub(crate) fn probe(
    result: Result<()>,
) -> (Arc<dyn FileSystem>, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (started_tx, started) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    (
        Arc::new(DeleteProbe {
            started: started_tx,
            release: Mutex::new(release_rx),
            result,
        }),
        started,
        release,
    )
}

impl FileSystem for DeleteProbe {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn init(
        _: &Url,
        _: Option<String>,
        _: Option<String>,
        _: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        unreachable!()
    }
    fn delete(&self, _: &str) -> Result<()> {
        self.started.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        self.result.clone()
    }
    fn delete_async(&self, _: &str) -> Result<()> {
        panic!("must track real delete completion")
    }
    fn create_dir(&self, _: &str) -> Result<()> {
        unreachable!()
    }
    fn exists(&self, _: &str) -> Result<bool> {
        unreachable!()
    }
    fn rename(&self, _: &str, _: &str) -> Result<()> {
        unreachable!()
    }
    fn list(&self, _: &str) -> Result<Vec<String>> {
        unreachable!()
    }
    fn open_read(&self, _: &str) -> Result<Box<dyn RandomAccessFile>> {
        unreachable!()
    }
    fn open_write(&self, _: &str) -> Result<Box<dyn SequentialWriteFile>> {
        unreachable!()
    }
    fn last_modified(&self, _: &str) -> Result<Option<u64>> {
        unreachable!()
    }
}
