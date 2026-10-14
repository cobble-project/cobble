use super::*;
use cobble::RandomAccessFile;
use std::any::Any;
use std::collections::HashMap;
use url::Url;

struct NonAtomicFileSystem(Arc<dyn FileSystem>);

impl FileSystem for NonAtomicFileSystem {
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
    fn create_dir(&self, path: &str) -> Result<()> {
        self.0.create_dir(path)
    }
    fn exists(&self, path: &str) -> Result<bool> {
        self.0.exists(path)
    }
    fn delete(&self, path: &str) -> Result<()> {
        self.0.delete(path)
    }
    fn delete_async(&self, path: &str) -> Result<()> {
        self.0.delete_async(path)
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        if self.0.exists(to)? {
            return Err(Error::IoError("replacement rename unavailable".to_string()));
        }
        self.0.rename(from, to)
    }
    fn list(&self, path: &str) -> Result<Vec<String>> {
        self.0.list(path)
    }
    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        self.0.open_read(path)
    }
    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        self.0.open_write(path)
    }
    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        self.0.last_modified(path)
    }
}

#[test]
fn non_atomic_catalog_replaces_mutable_files_only() {
    let dir = tempfile::tempdir().unwrap();
    let inner = FileSystemRegistry::new()
        .get_or_register(format!("file://{}", dir.path().display()))
        .unwrap();
    let file_system: Arc<dyn FileSystem> = Arc::new(NonAtomicFileSystem(inner));
    let store = CatalogStore {
        file_system: file_system.clone(),
        prefix: "catalog".to_string(),
    };
    for generation in [1, 2] {
        let manifest = format!("CATALOG-{generation}");
        store.write(&manifest, b"immutable").unwrap();
        store
            .replace("CURRENT", generation.to_string().as_bytes())
            .unwrap();
        assert_eq!(store.read(&manifest).unwrap(), b"immutable"[..]);
        assert!(file_system.exists(&format!("catalog/{manifest}")).unwrap());
        assert!(
            !file_system
                .exists(&format!("catalog/{manifest}-ATOMIC/"))
                .unwrap()
        );
    }
    assert_eq!(store.read_replaced("CURRENT").unwrap(), b"2"[..]);
    assert!(store.replaced_exists("CURRENT").unwrap());
    assert!(!file_system.exists("catalog/CURRENT").unwrap());
    let namespace_current = "namespaces/00000000-0000-0000-0000-000000000001/CURRENT";
    for value in [b"1", b"2"] {
        store.replace(namespace_current, value).unwrap();
    }
    assert_eq!(store.read_replaced(namespace_current).unwrap(), b"2"[..]);
}
