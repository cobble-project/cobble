use super::*;

#[test]
fn test_strip_and_verify_round_trip() {
    let payload = br#"{"hello":"world"}"#;
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    let decoded = strip_and_verify(&encoded).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn test_strip_and_verify_rejects_checksum_mismatch() {
    let payload = br#"{"hello":"world"}"#;
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    encoded[1] ^= 0xff;
    let err = strip_and_verify(&encoded).unwrap_err();
    assert!(matches!(err, Error::ChecksumMismatch(_)));
}

#[derive(Default)]
struct TrailerWriter {
    bytes: Vec<u8>,
    fail_after: Option<usize>,
    zero_write: bool,
    fail_close: bool,
    close_calls: usize,
}

impl File for TrailerWriter {
    fn close(&mut self) -> Result<()> {
        self.close_calls += 1;
        if self.fail_close {
            Err(Error::IoError("injected close error".into()))
        } else {
            Ok(())
        }
    }
    fn size(&self) -> usize {
        self.bytes.len()
    }
}

impl SequentialWriteFile for TrailerWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.zero_write {
            return Ok(0);
        }
        if self
            .fail_after
            .is_some_and(|limit| self.bytes.len() >= limit)
        {
            return Err(Error::IoError("injected trailer error".into()));
        }
        let count = data.len().min(2).min(
            self.fail_after
                .map_or(usize::MAX, |limit| limit - self.bytes.len()),
        );
        self.bytes.extend_from_slice(&data[..count]);
        Ok(count)
    }
}

#[test]
fn metadata_writer_short_writes_checksum_and_idempotent_close() {
    let mut writer = MetadataWriter::new(TrailerWriter::default());
    write_all(&mut writer, b"payload").unwrap();
    writer.close().unwrap();
    assert_eq!(strip_and_verify(&writer.inner.bytes).unwrap(), b"payload");
    assert_eq!(writer.size(), 7 + METADATA_CHECKSUM_TRAILER_SIZE);
    writer.close().unwrap();
    assert_eq!(writer.inner.close_calls, 1);
    assert!(writer.write(b"late").is_err());
}

#[test]
fn metadata_writer_failed_trailer_closes_and_caches_primary_error() {
    for zero_write in [false, true] {
        let inner = TrailerWriter {
            fail_after: Some(3),
            fail_close: true,
            zero_write,
            ..Default::default()
        };
        let mut writer = MetadataWriter::new(inner);
        let error = writer.close().unwrap_err();
        assert!(!error.to_string().contains("injected close error"));
        let size = writer.size();
        assert_eq!(writer.close().unwrap_err().to_string(), error.to_string());
        assert_eq!(writer.size(), size);
        assert_eq!(writer.inner.close_calls, 1);
    }
}
