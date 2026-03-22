use crate::block_cache::BlockCache;
use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::iterator::KvIterator;
use crate::parquet::file_adapter::RandomAccessChunkReader;
use crate::parquet::meta::{ParquetRowGroupRange, decode_meta_row_group_ranges};
use crate::sst::row_codec::encode_value;
use crate::r#type::{Column, KvValue, Value, ValueType};
use bytes::Bytes;
use parquet::column::reader::{ColumnReaderImpl, get_typed_column_reader};
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::sync::Arc;

const PARQUET_ITERATOR_BATCH_ROWS: usize = 256;

type ByteArrayColumnReader = ColumnReaderImpl<ByteArrayType>;
type Int64ColumnReader = ColumnReaderImpl<Int64Type>;

pub(crate) struct ParquetIterator {
    file: Arc<dyn RandomAccessFile>,
    file_id: Option<u64>,
    block_cache: Option<BlockCache>,
    row_group_ranges: Option<Vec<ParquetRowGroupRange>>,
    total_row_groups: usize,
    current_row_group_idx: usize,
    current_group_rows: usize,
    current_group_rows_loaded: usize,
    num_columns: usize,
    key_reader: Option<ByteArrayColumnReader>,
    expired_at_reader: Option<Int64ColumnReader>,
    column_readers: Vec<Option<ByteArrayColumnReader>>,
    batch_keys: Vec<ByteArray>,
    batch_values: Vec<Bytes>,
    current_idx: Option<usize>,
    previous_key: Option<Bytes>,
}

impl ParquetIterator {
    pub(crate) fn new(file: Box<dyn RandomAccessFile>) -> Result<Self> {
        Self::new_with_ranges(file, None, None, None)
    }

    pub(crate) fn from_data_file(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        let row_group_ranges = decode_meta_row_group_ranges(data_file.meta_bytes())?;
        Self::new_with_ranges(file, row_group_ranges, Some(data_file.file_id), block_cache)
    }

    fn new_with_ranges(
        file: Box<dyn RandomAccessFile>,
        row_group_ranges: Option<Vec<ParquetRowGroupRange>>,
        file_id: Option<u64>,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        let file = Arc::from(file);
        let total_row_groups = Self::row_group_count(&file, file_id, block_cache.clone())?;
        let row_group_ranges = row_group_ranges.filter(|ranges| ranges.len() == total_row_groups);
        let num_columns = Self::num_columns(&file, file_id, block_cache.clone())?;
        Ok(Self {
            file,
            file_id,
            block_cache,
            row_group_ranges,
            total_row_groups,
            current_row_group_idx: 0,
            current_group_rows: 0,
            current_group_rows_loaded: 0,
            num_columns,
            key_reader: None,
            expired_at_reader: None,
            column_readers: (0..num_columns).map(|_| None).collect(),
            batch_keys: Vec::new(),
            batch_values: Vec::new(),
            current_idx: None,
            previous_key: None,
        })
    }

    fn num_columns(
        file: &Arc<dyn RandomAccessFile>,
        file_id: Option<u64>,
        block_cache: Option<BlockCache>,
    ) -> Result<usize> {
        let chunk_reader =
            RandomAccessChunkReader::from_arc_with_cache(Arc::clone(file), file_id, block_cache);
        let reader = SerializedFileReader::new(chunk_reader)
            .map_err(|err| Error::IoError(format!("Failed to open parquet reader: {}", err)))?;
        let metadata = reader.metadata();
        if metadata.num_row_groups() == 0 {
            return Ok(1);
        }
        let schema_descr = metadata.file_metadata().schema_descr();
        let num_fields = schema_descr.num_columns();
        if num_fields < 3 {
            return Err(Error::IoError(format!(
                "Unsupported parquet kv schema column count: {}",
                num_fields
            )));
        }
        Ok(num_fields - 2)
    }

    fn row_group_count(
        file: &Arc<dyn RandomAccessFile>,
        file_id: Option<u64>,
        block_cache: Option<BlockCache>,
    ) -> Result<usize> {
        let chunk_reader =
            RandomAccessChunkReader::from_arc_with_cache(Arc::clone(file), file_id, block_cache);
        let reader = SerializedFileReader::new(chunk_reader)
            .map_err(|err| Error::IoError(format!("Failed to open parquet reader: {}", err)))?;
        let metadata = reader.metadata();
        let count = metadata.num_row_groups();
        if count == 0 {
            return Ok(0);
        }
        for idx in 0..count {
            let row_group = reader.get_row_group(idx).map_err(|err| {
                Error::IoError(format!("Failed to get parquet row group {}: {}", idx, err))
            })?;
            if row_group.num_columns() < 3 {
                return Err(Error::IoError(format!(
                    "Unsupported parquet kv column count: {}",
                    row_group.num_columns()
                )));
            }
        }
        Ok(count)
    }

    fn open_row_group_readers(
        file: &Arc<dyn RandomAccessFile>,
        row_group_idx: usize,
        num_columns: usize,
        file_id: Option<u64>,
        block_cache: Option<BlockCache>,
    ) -> Result<(
        usize,
        ByteArrayColumnReader,
        Int64ColumnReader,
        Vec<ByteArrayColumnReader>,
    )> {
        let chunk_reader =
            RandomAccessChunkReader::from_arc_with_cache(Arc::clone(file), file_id, block_cache);
        let reader = SerializedFileReader::new(chunk_reader)
            .map_err(|err| Error::IoError(format!("Failed to open parquet reader: {}", err)))?;
        let row_group = reader.get_row_group(row_group_idx).map_err(|err| {
            Error::IoError(format!(
                "Failed to get parquet row group {}: {}",
                row_group_idx, err
            ))
        })?;
        if row_group.num_columns() != num_columns + 2 {
            return Err(Error::IoError(format!(
                "Unsupported parquet kv column count: {}",
                row_group.num_columns()
            )));
        }
        let row_count = row_group.metadata().num_rows() as usize;
        let key_reader = row_group
            .get_column_reader(0)
            .map_err(|err| Error::IoError(format!("Failed to get key column reader: {}", err)))
            .map(get_typed_column_reader::<ByteArrayType>)?;
        let expired_at_reader = row_group
            .get_column_reader(1)
            .map_err(|err| Error::IoError(format!("Failed to get expired_at reader: {}", err)))
            .map(get_typed_column_reader::<Int64Type>)?;
        let mut column_readers = Vec::with_capacity(num_columns);
        for idx in 0..num_columns {
            let reader = row_group
                .get_column_reader(idx + 2)
                .map_err(|err| {
                    Error::IoError(format!(
                        "Failed to get payload reader for column {}: {}",
                        idx, err
                    ))
                })
                .map(get_typed_column_reader::<ByteArrayType>)?;
            column_readers.push(reader);
        }
        Ok((row_count, key_reader, expired_at_reader, column_readers))
    }

    fn reset_readers_from_group(&mut self, start_row_group_idx: usize) {
        self.current_row_group_idx = start_row_group_idx.min(self.total_row_groups);
        self.current_group_rows = 0;
        self.current_group_rows_loaded = 0;
        self.key_reader = None;
        self.expired_at_reader = None;
        self.column_readers = (0..self.num_columns).map(|_| None).collect();
        self.batch_keys.clear();
        self.batch_values.clear();
        self.current_idx = None;
        self.previous_key = None;
    }

    fn ensure_active_row_group(&mut self) -> Result<bool> {
        while self.current_row_group_idx < self.total_row_groups {
            if self.key_reader.is_some()
                && self.expired_at_reader.is_some()
                && self.column_readers.iter().all(Option::is_some)
                && self.current_group_rows_loaded < self.current_group_rows
            {
                return Ok(true);
            }
            let (rows, key_reader, expired_at_reader, column_readers) =
                Self::open_row_group_readers(
                    &self.file,
                    self.current_row_group_idx,
                    self.num_columns,
                    self.file_id,
                    self.block_cache.clone(),
                )?;
            self.current_group_rows = rows;
            self.current_group_rows_loaded = 0;
            self.key_reader = Some(key_reader);
            self.expired_at_reader = Some(expired_at_reader);
            self.column_readers = column_readers.into_iter().map(Some).collect();
            if rows > 0 {
                return Ok(true);
            }
            self.current_row_group_idx += 1;
            self.key_reader = None;
            self.expired_at_reader = None;
            self.column_readers = (0..self.num_columns).map(|_| None).collect();
        }
        Ok(false)
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        loop {
            if !self.ensure_active_row_group()? {
                self.batch_keys.clear();
                self.batch_values.clear();
                self.current_idx = None;
                return Ok(false);
            }
            let to_read = (self.current_group_rows - self.current_group_rows_loaded)
                .min(PARQUET_ITERATOR_BATCH_ROWS);
            self.batch_keys.clear();
            self.batch_values.clear();

            let key_reader = self
                .key_reader
                .as_mut()
                .ok_or_else(|| Error::IoError("Missing parquet key reader".to_string()))?;
            let (key_records, key_values, _) = key_reader
                .read_records(to_read, None, None, &mut self.batch_keys)
                .map_err(|err| {
                    Error::IoError(format!("Failed to read key column records: {}", err))
                })?;
            if key_records == 0 {
                self.current_row_group_idx += 1;
                self.key_reader = None;
                self.expired_at_reader = None;
                self.column_readers = (0..self.num_columns).map(|_| None).collect();
                continue;
            }

            let expired_at_reader = self
                .expired_at_reader
                .as_mut()
                .ok_or_else(|| Error::IoError("Missing parquet expired_at reader".to_string()))?;
            let mut batch_expired_ats = Vec::with_capacity(key_records);
            let (expired_records, expired_values, _) = expired_at_reader
                .read_records(key_records, None, None, &mut batch_expired_ats)
                .map_err(|err| {
                    Error::IoError(format!("Failed to read expired_at column records: {}", err))
                })?;
            if expired_records != key_records || expired_values != key_values {
                return Err(Error::IoError(format!(
                    "Parquet key/expired_at batch mismatch: key_records={} expired_records={} key_values={} expired_values={}",
                    key_records, expired_records, key_values, expired_values
                )));
            }

            let mut payload_values_per_column: Vec<Vec<ByteArray>> =
                Vec::with_capacity(self.num_columns);
            let mut payload_def_levels_per_column: Vec<Vec<i16>> =
                Vec::with_capacity(self.num_columns);
            for idx in 0..self.num_columns {
                let reader = self.column_readers[idx].as_mut().ok_or_else(|| {
                    Error::IoError(format!("Missing parquet payload reader {}", idx))
                })?;
                let mut values = Vec::new();
                let mut def_levels = Vec::new();
                let (records, _, _) = reader
                    .read_records(key_records, Some(&mut def_levels), None, &mut values)
                    .map_err(|err| {
                        Error::IoError(format!(
                            "Failed to read parquet payload column {} records: {}",
                            idx, err
                        ))
                    })?;
                if records != key_records {
                    return Err(Error::IoError(format!(
                        "Parquet payload records mismatch at column {}: expected {} got {}",
                        idx, key_records, records
                    )));
                }
                payload_values_per_column.push(values);
                payload_def_levels_per_column.push(def_levels);
            }

            self.batch_values.clear();
            self.batch_values.reserve(key_records);
            let mut payload_offsets = vec![0usize; self.num_columns];
            for (row_idx, &expired_at) in batch_expired_ats.iter().enumerate().take(key_records) {
                let mut cols = Vec::with_capacity(self.num_columns);
                for col_idx in 0..self.num_columns {
                    let is_present = payload_def_levels_per_column[col_idx]
                        .get(row_idx)
                        .copied()
                        .unwrap_or(0)
                        > 0;
                    if !is_present {
                        cols.push(None);
                        continue;
                    }
                    let payload_idx = payload_offsets[col_idx];
                    payload_offsets[col_idx] = payload_idx + 1;
                    let payload = payload_values_per_column[col_idx]
                        .get(payload_idx)
                        .ok_or_else(|| {
                            Error::IoError(format!(
                                "Parquet payload value missing at column {} row {}",
                                col_idx, row_idx
                            ))
                        })?
                        .data();
                    if payload.is_empty() {
                        return Err(Error::IoError(format!(
                            "Parquet payload is empty at column {} row {}",
                            col_idx, row_idx
                        )));
                    }
                    let value_type = ValueType::decode_tag(payload[0])?;
                    cols.push(Some(Column::new(
                        value_type,
                        Bytes::copy_from_slice(&payload[1..]),
                    )));
                }
                let value = Value::new_with_expired_at(
                    cols,
                    if expired_at <= 0 {
                        None
                    } else {
                        Some(expired_at as u32)
                    },
                );
                self.batch_values
                    .push(encode_value(&value, self.num_columns));
            }

            if self.batch_keys.len() != self.batch_values.len() {
                return Err(Error::IoError(format!(
                    "Parquet key/value row count mismatch: {} vs {}",
                    self.batch_keys.len(),
                    self.batch_values.len()
                )));
            }
            if let (Some(prev), Some(first)) = (self.previous_key.as_ref(), self.batch_keys.first())
                && prev.as_ref() >= first.data()
            {
                return Err(Error::IoError(
                    "Parquet rows are not strictly sorted by key".to_string(),
                ));
            }
            for idx in 1..self.batch_keys.len() {
                if self.batch_keys[idx - 1].data() >= self.batch_keys[idx].data() {
                    return Err(Error::IoError(
                        "Parquet rows are not strictly sorted by key".to_string(),
                    ));
                }
            }

            self.current_group_rows_loaded += key_records;
            if self.current_group_rows_loaded >= self.current_group_rows {
                self.current_row_group_idx += 1;
            }
            self.current_idx = (!self.batch_keys.is_empty()).then_some(0);
            return Ok(self.current_idx.is_some());
        }
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        let start_group = self
            .row_group_ranges
            .as_ref()
            .map(|ranges| ranges.partition_point(|range| range.end_key.as_slice() < target))
            .unwrap_or(0);
        self.reset_readers_from_group(start_group);
        loop {
            if !self.load_next_batch()? {
                return Ok(());
            }
            let Some(last_key) = self.batch_keys.last() else {
                continue;
            };
            if last_key.data() < target {
                self.previous_key = Some(Bytes::copy_from_slice(last_key.data()));
                continue;
            }
            let idx = self.batch_keys.partition_point(|key| key.data() < target);
            self.current_idx = (idx < self.batch_keys.len()).then_some(idx);
            return Ok(());
        }
    }

    pub(crate) fn seek_to_first(&mut self) -> Result<()> {
        self.reset_readers_from_group(0);
        let _ = self.load_next_batch()?;
        Ok(())
    }

    pub(crate) fn next(&mut self) -> Result<bool> {
        let Some(current) = self.current_idx else {
            return Ok(false);
        };
        let next = current + 1;
        if next < self.batch_keys.len() {
            self.current_idx = Some(next);
            return Ok(true);
        }
        if let Some(last_key) = self.batch_keys.last() {
            self.previous_key = Some(Bytes::copy_from_slice(last_key.data()));
        }
        self.load_next_batch()
    }

    pub(crate) fn valid(&self) -> bool {
        self.current_idx
            .map(|idx| idx < self.batch_keys.len())
            .unwrap_or(false)
    }

    pub(crate) fn key(&self) -> Result<Option<Bytes>> {
        Ok(self
            .current_idx
            .map(|idx| Bytes::copy_from_slice(self.batch_keys[idx].data())))
    }

    pub(crate) fn value(&self) -> Result<Option<Bytes>> {
        Ok(self.current_idx.map(|idx| self.batch_values[idx].clone()))
    }
}

impl<'a> KvIterator<'a> for ParquetIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        ParquetIterator::seek(self, target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        ParquetIterator::seek_to_first(self)
    }

    fn next(&mut self) -> Result<bool> {
        ParquetIterator::next(self)
    }

    fn valid(&self) -> bool {
        ParquetIterator::valid(self)
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            return Ok(self.batch_keys.get(idx).map(|key| key.data()));
        }
        Ok(None)
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        ParquetIterator::key(self)
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(ParquetIterator::value(self)?.map(KvValue::Encoded))
    }
}
