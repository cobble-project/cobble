//! Atomic replacement of mutable metadata, including filesystems without atomic rename.
//!
//! There must be a single writer per logical path, and clients sharing storage must agree on
//! its atomic-replacement capability. On non-atomic filesystems a closed,
//! immutable payload is published by creating an empty completion marker. The marker name
//! records its length and checksum, so plain metadata retains its original payload format.
use crate::error::{Error, Result};
use crate::file::{File, FileSystem, RandomAccessFile, SequentialWriteFile};
use bytes::Bytes;
use crc32fast::Hasher;
use std::sync::Arc;
use uuid::Uuid;

const SUFFIX: &str = "-ATOMIC";

fn directory(path: &str) -> String {
    format!("{path}{SUFFIX}")
}

fn entries(fs: &dyn FileSystem, path: &str) -> Result<Vec<String>> {
    if fs.exists(&format!("{}/", path.trim_end_matches('/')))? {
        fs.list(path)
    } else {
        Ok(Vec::new())
    }
}

fn completed(fs: &dyn FileSystem, path: &str) -> Result<Option<(u64, usize, u32)>> {
    let mut latest = None;
    for entry in entries(fs, &directory(path))? {
        let name = entry.rsplit('/').next().unwrap_or(&entry);
        let Some((sequence, integrity)) = name.split_once(".complete-") else {
            continue;
        };
        let Some((length, checksum)) = integrity.split_once('-') else {
            continue;
        };
        if let (Ok(sequence), Ok(length), Ok(checksum)) = (
            sequence.parse::<u64>(),
            length.parse::<usize>(),
            u32::from_str_radix(checksum, 16),
        ) && latest.is_none_or(|(previous, _, _)| sequence > previous)
        {
            latest = Some((sequence, length, checksum));
        }
    }
    Ok(latest)
}

pub fn exists(fs: &dyn FileSystem, path: &str) -> Result<bool> {
    if fs.supports_atomic_replace() {
        return Ok(fs.exists(path)? || completed(fs, path)?.is_some());
    }
    Ok(completed(fs, path)?.is_some() || fs.exists(path)?)
}

/// Read the selected commit in full, retrying resolution if cleanup removed an old payload.
/// A corrupt selected commit is an error, never a reason to read an older format or version.
pub fn open_read(fs: &dyn FileSystem, path: &str) -> Result<Box<dyn RandomAccessFile>> {
    loop {
        let selected = if fs.supports_atomic_replace() && fs.exists(path)? {
            None
        } else {
            completed(fs, path)?
        };
        let Some((sequence, length, checksum)) = selected else {
            match fs
                .open_read(path)
                .and_then(|file| file.read_at(0, file.size()))
            {
                Ok(bytes) => return Ok(Box::new(MetadataBytes(bytes))),
                Err(err) => {
                    if !fs.exists(path)? && completed(fs, path)?.is_some() {
                        continue;
                    }
                    return Err(err);
                }
            }
        };
        let payload_path = format!("{}/{sequence:020}.data", directory(path));
        let bytes = fs
            .open_read(&payload_path)
            .and_then(|file| file.read_at(0, file.size()));
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(err) => {
                let latest = completed(fs, path)?;
                if latest.map(|(latest, _, _)| latest) != Some(sequence)
                    && (latest.is_some() || fs.exists(path)?)
                {
                    continue;
                }
                return Err(err);
            }
        };
        if bytes.len() != length || crc32fast::hash(&bytes) != checksum {
            return Err(Error::ChecksumMismatch(format!(
                "invalid atomic metadata commit {payload_path}"
            )));
        }
        return Ok(Box::new(MetadataBytes(bytes)));
    }
}

struct MetadataBytes(Bytes);

impl File for MetadataBytes {
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
    fn size(&self) -> usize {
        self.0.len()
    }
}

impl RandomAccessFile for MetadataBytes {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        let end = offset
            .checked_add(size)
            .filter(|end| *end <= self.0.len())
            .ok_or_else(|| Error::IoError("metadata read out of bounds".to_string()))?;
        Ok(self.0.slice(offset..end))
    }
}

fn delete_versions(fs: &dyn FileSystem, path: &str) -> Result<()> {
    let dir = directory(path);
    for entry in entries(fs, &dir)? {
        let name = entry.rsplit('/').next().unwrap_or(&entry);
        fs.delete(&format!("{dir}/{name}"))?;
    }
    let dir = format!("{dir}/");
    if fs.exists(&dir)? {
        fs.delete(&dir)?;
    }
    Ok(())
}

/// A streaming publication writer. Payload bytes are not transformed.
pub struct ReplaceWriter {
    fs: Arc<dyn FileSystem>,
    path: String,
    payload_path: String,
    sequence: Option<u64>,
    writer: Option<Box<dyn SequentialWriteFile>>,
    checksum: Hasher,
    length: usize,
}

impl ReplaceWriter {
    pub fn new(fs: Arc<dyn FileSystem>, path: &str) -> Result<Self> {
        let (payload_path, sequence) = if fs.supports_atomic_replace() {
            (format!("{path}.tmp-{}", Uuid::new_v4()), None)
        } else {
            let dir = directory(path);
            fs.create_dir(&dir)?;
            let sequence = entries(fs.as_ref(), &dir)?
                .iter()
                .filter_map(|entry| {
                    entry
                        .rsplit('/')
                        .next()?
                        .split('.')
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::InvalidState("atomic metadata sequence exhausted".to_string())
                })?;
            (format!("{dir}/{sequence:020}.data"), Some(sequence))
        };
        let writer = fs.open_write(&payload_path)?;
        Ok(Self {
            fs,
            path: path.to_string(),
            payload_path,
            sequence,
            writer: Some(writer),
            checksum: Hasher::new(),
            length: 0,
        })
    }
}

impl SequentialWriteFile for ReplaceWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::IoError("atomic writer already closed".to_string()))?;
        let mut offset = 0;
        while offset < data.len() {
            let count = writer.write(&data[offset..])?;
            if count == 0 {
                return Err(Error::IoError(format!(
                    "metadata write returned zero bytes: {}",
                    self.path
                )));
            }
            if self.sequence.is_some() {
                self.checksum.update(&data[offset..offset + count]);
            }
            self.length += count;
            offset += count;
        }
        Ok(offset)
    }
}

impl Drop for ReplaceWriter {
    fn drop(&mut self) {
        self.writer.take();
        // Never remove a versioned payload here: its marker may already have published.
        if self.sequence.is_none() && self.fs.exists(&self.payload_path).unwrap_or(false) {
            let _ = self.fs.delete(&self.payload_path);
        }
    }
}

impl File for ReplaceWriter {
    fn size(&self) -> usize {
        self.length
    }

    fn close(&mut self) -> Result<()> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };
        writer.close()?;
        if let Some(sequence) = self.sequence {
            let checksum = self.checksum.clone().finalize();
            let marker = format!(
                "{}/{sequence:020}.complete-{}-{checksum:08x}",
                directory(&self.path),
                self.length
            );
            self.fs.open_write(&marker)?.close()?;
            // Publication succeeded. Remove the secondary format and older commits only now.
            let cleanup = (|| -> Result<()> {
                if self.fs.exists(&self.path)? {
                    self.fs.delete(&self.path)?;
                }
                for entry in entries(self.fs.as_ref(), &directory(&self.path))? {
                    let name = entry.rsplit('/').next().unwrap_or(&entry);
                    if name
                        .split('.')
                        .next()
                        .and_then(|s| s.parse::<u64>().ok())
                        .is_some_and(|s| s < sequence)
                    {
                        self.fs
                            .delete(&format!("{}/{}", directory(&self.path), name))?;
                    }
                }
                Ok(())
            })();
            if let Err(err) = cleanup {
                log::warn!("atomic metadata cleanup failed for {}: {err}", self.path);
            }
        } else {
            self.fs.rename(&self.payload_path, &self.path)?;
            if let Err(err) = delete_versions(self.fs.as_ref(), &self.path) {
                log::warn!("atomic metadata cleanup failed for {}: {err}", self.path);
            }
        }
        Ok(())
    }
}

pub fn write(fs: Arc<dyn FileSystem>, path: &str, payload: &[u8]) -> Result<()> {
    let mut writer = ReplaceWriter::new(fs, path)?;
    writer.write(payload)?;
    writer.close()
}

#[cfg(test)]
#[path = "../../tests/unit/file/atomic_replace.rs"]
pub(crate) mod tests;
