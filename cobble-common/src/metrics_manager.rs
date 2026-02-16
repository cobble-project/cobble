use crate::compaction::CompactionTaskMetrics;
use crate::file::FileManagerMetrics;
use crate::memtable::MemtableManagerMetrics;
use crate::sst::{SSTIteratorMetrics, SSTWriterMetrics, SstCompressionAlgorithm};
use std::sync::Arc;

#[derive(Clone)]
pub struct MetricsManager {
    db_id: String,
    sst_iterator_metrics: Arc<SSTIteratorMetrics>,
    compaction_metrics: Arc<CompactionTaskMetrics>,
    memtable_metrics: MemtableManagerMetrics,
    file_manager_metrics: FileManagerMetrics,
    sst_writer_metrics_none: SSTWriterMetrics,
    sst_writer_metrics_lz4: SSTWriterMetrics,
}

impl MetricsManager {
    pub(crate) fn new(db_id: String) -> Self {
        let sst_iterator_metrics = Arc::new(SSTIteratorMetrics::new(&db_id));
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new(&db_id));
        let memtable_metrics = MemtableManagerMetrics::new(&db_id);
        let file_manager_metrics = FileManagerMetrics::new(&db_id);
        let sst_writer_metrics_none = SSTWriterMetrics::new(&db_id, SstCompressionAlgorithm::None);
        let sst_writer_metrics_lz4 = SSTWriterMetrics::new(&db_id, SstCompressionAlgorithm::Lz4);
        Self {
            db_id,
            sst_iterator_metrics,
            compaction_metrics,
            memtable_metrics,
            file_manager_metrics,
            sst_writer_metrics_none,
            sst_writer_metrics_lz4,
        }
    }

    pub(crate) fn db_id(&self) -> &str {
        &self.db_id
    }

    pub(crate) fn sst_iterator_metrics(&self) -> Arc<SSTIteratorMetrics> {
        Arc::clone(&self.sst_iterator_metrics)
    }

    pub(crate) fn compaction_metrics(&self) -> Arc<CompactionTaskMetrics> {
        Arc::clone(&self.compaction_metrics)
    }

    pub(crate) fn memtable_metrics(&self) -> MemtableManagerMetrics {
        self.memtable_metrics.clone()
    }

    pub(crate) fn file_manager_metrics(&self) -> FileManagerMetrics {
        self.file_manager_metrics.clone()
    }

    pub(crate) fn sst_writer_metrics(
        &self,
        compression: SstCompressionAlgorithm,
    ) -> SSTWriterMetrics {
        match compression {
            SstCompressionAlgorithm::None => self.sst_writer_metrics_none.clone(),
            SstCompressionAlgorithm::Lz4 => self.sst_writer_metrics_lz4.clone(),
        }
    }
}
