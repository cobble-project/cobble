use crate::error::Result;
use crate::merge_operator::{MergeOperator, default_merge_operator, default_merge_operator_ref};
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Runtime schema for merge semantics.
#[derive(Clone)]
pub struct Schema {
    version: u64,
    num_columns: usize,
    operators: Arc<Vec<Arc<dyn MergeOperator>>>,
}

impl Schema {
    pub(crate) fn new(
        version: u64,
        num_columns: usize,
        mut operators: Vec<Arc<dyn MergeOperator>>,
    ) -> Self {
        if operators.len() < num_columns {
            operators.resize_with(num_columns, default_merge_operator);
        } else if operators.len() > num_columns {
            operators.truncate(num_columns);
        }
        Self {
            version,
            num_columns,
            operators: Arc::new(operators),
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub(crate) fn operator(&self, column_idx: usize) -> &dyn MergeOperator {
        self.operators
            .get(column_idx)
            .unwrap_or_else(|| default_merge_operator_ref())
            .as_ref()
    }

    pub(crate) fn operator_ids(&self, num_columns: usize) -> Vec<String> {
        (0..num_columns)
            .map(|column_idx| self.operator(column_idx).id())
            .collect()
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self::new(0, 0, Vec::new()))
    }
}

/// Manages versioned runtime schema snapshots.
#[derive(Clone)]
pub(crate) struct SchemaManager {
    schema: Arc<ArcSwap<Schema>>,
    next_version: Arc<AtomicU64>,
}

impl SchemaManager {
    pub(crate) fn new(num_columns: usize) -> Self {
        let initial = Arc::new(Schema::new(
            0,
            num_columns,
            vec![default_merge_operator(); num_columns],
        ));
        Self {
            schema: Arc::new(ArcSwap::from(initial)),
            next_version: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn latest_schema(&self) -> Arc<Schema> {
        self.schema.load_full()
    }

    pub(crate) fn builder(self: &Arc<Self>) -> SchemaBuilder {
        self.builder_from(self.latest_schema())
    }

    pub(crate) fn builder_from(self: &Arc<Self>, schema: Arc<Schema>) -> SchemaBuilder {
        SchemaBuilder::from_schema(Arc::clone(self), schema)
    }

    fn commit_build(
        &self,
        num_columns: usize,
        operators: Vec<Arc<dyn MergeOperator>>,
    ) -> Arc<Schema> {
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        let schema = Arc::new(Schema::new(version, num_columns, operators));
        self.schema.store(Arc::clone(&schema));
        schema
    }
}

pub struct SchemaBuilder {
    manager: Arc<SchemaManager>,
    num_columns: usize,
    operators: Vec<Arc<dyn MergeOperator>>,
}

impl SchemaBuilder {
    fn from_schema(manager: Arc<SchemaManager>, schema: Arc<Schema>) -> Self {
        Self {
            manager,
            num_columns: schema.num_columns(),
            operators: schema.operators.as_ref().clone(),
        }
    }

    pub fn set_column_operator(
        &mut self,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        if self.operators.is_empty() && column_idx > 0 {
            self.operators = vec![default_merge_operator(); column_idx + 1];
        } else if column_idx >= self.operators.len() {
            self.operators
                .resize_with(column_idx + 1, default_merge_operator);
        }
        if column_idx >= self.num_columns {
            self.num_columns = column_idx + 1;
        }
        self.operators[column_idx] = operator;
        Ok(())
    }

    pub fn set_all(&mut self, operators: Vec<Arc<dyn MergeOperator>>) {
        self.operators = operators;
    }

    pub fn commit(self) -> Arc<Schema> {
        self.manager.commit_build(self.num_columns, self.operators)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_operator::MergeOperator;
    use bytes::Bytes;

    struct BracketMergeOperator;

    impl MergeOperator for BracketMergeOperator {
        fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
            let existing = existing_value.as_ref();
            Ok(format!(
                "[{}+{}]",
                String::from_utf8_lossy(existing),
                String::from_utf8_lossy(value.as_ref())
            )
            .into_bytes()
            .into())
        }
    }

    #[test]
    fn test_schema_builder_set_column_operator() {
        let manager = Arc::new(SchemaManager::new(2));
        let mut builder = manager.builder();
        builder
            .set_column_operator(1, Arc::new(BracketMergeOperator))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.version(), 1);
        let op = schema.operator(1);
        let merged = op
            .merge(Bytes::from_static(b"x"), Bytes::from_static(b"y"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"[x+y]");
    }

    #[test]
    fn test_schema_falls_back_to_default_for_missing_column() {
        let manager = SchemaManager::new(1);
        let schema = manager.latest_schema();
        let op = schema.operator(10);
        let merged = op
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"ab");
    }

    #[test]
    fn test_schema_builder_extends_columns() {
        let manager = Arc::new(SchemaManager::new(1));
        let base = manager.latest_schema();
        let mut builder = manager.builder_from(base);
        builder
            .set_column_operator(3, Arc::new(BracketMergeOperator))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.num_columns(), 4);
        let merged0 = schema
            .operator(0)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        let merged3 = schema
            .operator(3)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged0.as_ref(), b"ab");
        assert_eq!(merged3.as_ref(), b"[a+b]");
    }
}
