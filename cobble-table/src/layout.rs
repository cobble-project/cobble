use crate::{FieldId, Result, TableError, TableSchema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub const LAYOUT_VERSION_CURRENT: u32 = 1;
pub const COBBLE_TABLE_CODEC_V1: &str = "cobble-table-v1";

/// Stable SHA-256 identity of a schema and its compiled physical layout.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LayoutFingerprint(String);

impl LayoutFingerprint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Storage semantics of a value column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueStorage {
    Replace,
}

/// Mapping of one semantic field to one Cobble value column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueColumnLayout {
    pub field_id: FieldId,
    pub column_index: u16,
    pub storage: ValueStorage,
}

/// Persisted physical mapping compiled from a [`TableSchema`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordLayoutDescriptor {
    pub version: u32,
    pub codec: String,
    pub schema_version: u64,
    pub key_fields: Vec<FieldId>,
    pub bucket_fields: Vec<FieldId>,
    pub value_columns: Vec<ValueColumnLayout>,
    pub fingerprint: LayoutFingerprint,
}

pub struct LayoutCompiler;

impl LayoutCompiler {
    pub fn compile(schema: &TableSchema) -> Result<RecordLayoutDescriptor> {
        schema.validate()?;
        let key_fields = schema.primary_key.clone();
        let bucket_fields = schema.bucket_key.clone();
        let key_set = key_fields.iter().copied().collect::<HashSet<_>>();
        let mut value_columns = Vec::with_capacity(schema.fields.len() - key_set.len());
        for field in &schema.fields {
            if key_set.contains(&field.id) {
                continue;
            }
            let column_index = u16::try_from(value_columns.len()).map_err(|_| {
                TableError::InvalidLayout("table has more than 65536 value columns".to_string())
            })?;
            value_columns.push(ValueColumnLayout {
                field_id: field.id,
                column_index,
                storage: ValueStorage::Replace,
            });
        }
        let fingerprint = fingerprint(schema, &key_fields, &bucket_fields, &value_columns)?;
        Ok(RecordLayoutDescriptor {
            version: LAYOUT_VERSION_CURRENT,
            codec: COBBLE_TABLE_CODEC_V1.to_string(),
            schema_version: schema.version,
            key_fields,
            bucket_fields,
            value_columns,
            fingerprint,
        })
    }
}

impl RecordLayoutDescriptor {
    pub fn validate_against(&self, schema: &TableSchema) -> Result<()> {
        schema.validate()?;
        if self.version != LAYOUT_VERSION_CURRENT {
            return Err(TableError::InvalidLayout(format!(
                "unsupported layout version: {}",
                self.version
            )));
        }
        if self.codec != COBBLE_TABLE_CODEC_V1 {
            return Err(TableError::InvalidLayout(format!(
                "unsupported table codec: {}",
                self.codec
            )));
        }
        if self.schema_version != schema.version {
            return Err(TableError::InvalidLayout(format!(
                "layout schema version {} does not match schema version {}",
                self.schema_version, schema.version
            )));
        }
        if self.key_fields != schema.primary_key || self.bucket_fields != schema.bucket_key {
            return Err(TableError::InvalidLayout(
                "layout key fields do not match the table schema".to_string(),
            ));
        }

        let key_set = schema.primary_key.iter().copied().collect::<HashSet<_>>();
        let expected_value_fields = schema
            .fields
            .iter()
            .filter(|field| !key_set.contains(&field.id))
            .map(|field| field.id)
            .collect::<Vec<_>>();
        if self.value_columns.len() != expected_value_fields.len() {
            return Err(TableError::InvalidLayout(
                "layout does not map every value field".to_string(),
            ));
        }
        for (index, (column, field_id)) in self
            .value_columns
            .iter()
            .zip(expected_value_fields)
            .enumerate()
        {
            if column.column_index as usize != index
                || column.field_id != field_id
                || column.storage != ValueStorage::Replace
            {
                return Err(TableError::InvalidLayout(
                    "value columns must follow schema order with contiguous indices".to_string(),
                ));
            }
        }

        let expected = fingerprint(
            schema,
            &self.key_fields,
            &self.bucket_fields,
            &self.value_columns,
        )?;
        if self.fingerprint != expected {
            return Err(TableError::InvalidLayout(
                "layout fingerprint does not match its schema and mapping".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct FingerprintInput<'a> {
    metadata_version: u32,
    codec: &'static str,
    schema: &'a TableSchema,
    key_fields: &'a [FieldId],
    bucket_fields: &'a [FieldId],
    value_columns: &'a [ValueColumnLayout],
}

fn fingerprint(
    schema: &TableSchema,
    key_fields: &[FieldId],
    bucket_fields: &[FieldId],
    value_columns: &[ValueColumnLayout],
) -> Result<LayoutFingerprint> {
    let payload = serde_json::to_vec(&FingerprintInput {
        metadata_version: LAYOUT_VERSION_CURRENT,
        codec: COBBLE_TABLE_CODEC_V1,
        schema,
        key_fields,
        bucket_fields,
        value_columns,
    })?;
    let digest = Sha256::digest(payload);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(LayoutFingerprint(hex))
}
