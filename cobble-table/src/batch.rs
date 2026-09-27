//! Appending typed table operations to a shared core write batch.

use super::{Table, TableKey, encode_table_row};
use crate::{Result, Value};
use cobble::{WriteBatch, WriteOptions};

impl Table {
    /// Append one full row in schema field order without writing it yet.
    /// Tables sharing the same `Db` may append to one batch before submitting it
    /// with `Db::write_batch`. Encoding errors leave the batch unchanged.
    /// As with a raw `WriteBatch`, table schemas must remain unchanged between
    /// encoding the operations and submitting the batch.
    pub fn append_put(&self, batch: &mut WriteBatch, row: &[Value]) -> Result<()> {
        let (bucket, key, columns) = encode_table_row(&self.compiled, row)?;
        Self::append_columns(batch, bucket, &key, columns, &self.write_options);
        Ok(())
    }

    /// Append a full row with per-row TTL and this table's column family.
    /// Durability waiting is selected when submitting the batch through
    /// `Db::write_batch_with_options`, not by this operation's `await_durable`.
    pub fn append_put_with_options(
        &self,
        batch: &mut WriteBatch,
        row: &[Value],
        options: &WriteOptions,
    ) -> Result<()> {
        let (bucket, key, columns) = encode_table_row(&self.compiled, row)?;
        Self::append_columns(
            batch,
            bucket,
            &key,
            columns,
            &self.rebound_write_options(options),
        );
        Ok(())
    }

    /// Append all non-key fields in schema order, reusing an encoded table key.
    /// Encoding errors leave the batch unchanged. As with `append_put_with_options`,
    /// durability waiting is configured when submitting the batch.
    pub fn append_put_values(
        &self,
        batch: &mut WriteBatch,
        key: &TableKey,
        values: &[Value],
        options: &WriteOptions,
    ) -> Result<()> {
        let columns = self.encode_values(values)?;
        Self::append_columns(
            batch,
            key.bucket(),
            key.encoded(),
            columns,
            &self.rebound_write_options(options),
        );
        Ok(())
    }

    /// Append deletion of every physical column, including the key-only row marker.
    /// The batch must be submitted to this table's `Db`.
    pub fn append_delete(&self, batch: &mut WriteBatch, key: &TableKey) {
        for column in 0..self.compiled.physical_columns {
            batch.delete_with_options(
                key.bucket(),
                key.encoded(),
                column as u16,
                &self.write_options,
            );
        }
    }

    fn append_columns(
        batch: &mut WriteBatch,
        bucket: u16,
        key: &[u8],
        columns: Vec<Vec<u8>>,
        options: &WriteOptions,
    ) {
        for (column, value) in columns.into_iter().enumerate() {
            batch.put_with_options(bucket, key, column as u16, value, options);
        }
    }
}
