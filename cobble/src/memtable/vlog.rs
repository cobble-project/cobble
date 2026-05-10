use crate::error::Result;
use crate::r#type::{Column, KvValue, Value, ValueType};
use crate::vlog::{VlogEdit, VlogStore, VlogWriter};
use bytes::Bytes;
use std::sync::Arc;

fn rewrite_value_columns_for_flush(
    mut value: Value,
    vlog_store: &Arc<VlogStore>,
    writer: &mut Option<VlogWriter<Box<dyn crate::file::SequentialWriteFile>>>,
    edit: &mut Option<VlogEdit>,
) -> Result<(Value, usize)> {
    let mut separated = 0usize;
    for column in &mut value.columns {
        let Some(column) = column.as_mut() else {
            continue;
        };
        let separated_value_type = match column.value_type {
            ValueType::Put if vlog_store.should_separate(column.data().len()) => {
                Some(ValueType::PutSeparated)
            }
            ValueType::Merge if vlog_store.should_separate(column.data().len()) => {
                Some(ValueType::MergeSeparated)
            }
            _ => None,
        };
        let Some(value_type) = separated_value_type else {
            continue;
        };
        if writer.is_none() {
            let (new_writer, new_edit) = vlog_store.create_writer()?;
            *writer = Some(new_writer);
            *edit = Some(new_edit);
        }
        let pointer = writer
            .as_mut()
            .expect("writer exists after initialization")
            .add_value(column.data())?;
        *column = Column::new(value_type, Bytes::copy_from_slice(&pointer.to_bytes()));
        separated += 1;
    }
    Ok((value, separated))
}

pub(crate) fn rewrite_kv_value_for_flush(
    kv_value: KvValue,
    num_columns: usize,
    vlog_store: &Arc<VlogStore>,
    writer: &mut Option<VlogWriter<Box<dyn crate::file::SequentialWriteFile>>>,
    edit: &mut Option<VlogEdit>,
) -> Result<(KvValue, usize)> {
    match kv_value {
        KvValue::Decoded(value) => {
            let (value, separated) =
                rewrite_value_columns_for_flush(value, vlog_store, writer, edit)?;
            Ok((KvValue::Decoded(value), separated))
        }
        KvValue::Encoded(bytes) => {
            let value = KvValue::Encoded(bytes.clone()).into_decoded(num_columns)?;
            let (value, separated) =
                rewrite_value_columns_for_flush(value, vlog_store, writer, edit)?;
            if separated == 0 {
                Ok((KvValue::Encoded(bytes), 0))
            } else {
                Ok((KvValue::Decoded(value), separated))
            }
        }
    }
}
