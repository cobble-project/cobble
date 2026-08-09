pub(crate) mod file_adapter;
mod iterator;
mod meta;
mod writer;

pub(crate) use file_adapter::{
    RandomAccessChunkReader, cache_parquet_data_block, parquet_row_group_cache_keys,
};
#[allow(unused_imports)]
pub(crate) use iterator::{ParquetIterator, ParquetIteratorOptions};
#[allow(unused_imports)]
pub(crate) use meta::{decode_meta_row_count, decode_meta_row_group_ranges};
#[allow(unused_imports)]
pub(crate) use writer::{ParquetWriter, ParquetWriterOptions};

#[cfg(test)]
#[path = "../../tests/unit/parquet/mod.rs"]
mod tests;
