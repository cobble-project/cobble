//! File writer that ensures atomic writes by writing to a temporary file
//! and renaming it to the final path upon successful completion.
use crate::error::{Error, Result};
use crate::file::{File, FileSystem, SequentialWriteFile};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct MetadataWriter {
    temp_path: String,
    final_path: String,
    fs: Arc<dyn FileSystem>,
    writer: Option<Box<dyn SequentialWriteFile>>,
}

impl MetadataWriter {
    pub(crate) fn new(path: &str, fs: &Arc<dyn FileSystem>) -> Result<Self> {
        let temp_path = format!("{}.tmp-{}", path, Uuid::new_v4());
        let writer = fs.open_write(&temp_path)?;
        Ok(Self {
            temp_path,
            final_path: path.to_string(),
            fs: fs.clone(),
            writer: Some(writer),
        })
    }
}

impl SequentialWriteFile for MetadataWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        match self.writer.as_mut() {
            Some(writer) => writer.write(data),
            None => Err(Error::IoError("Atomic writer already closed".to_string())),
        }
    }
}

impl File for MetadataWriter {
    fn close(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let mut writer = writer;
        writer.close()?;
        self.fs.rename(&self.temp_path, &self.final_path)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.writer.as_ref().map(|w| w.size()).unwrap_or(0)
    }
}
