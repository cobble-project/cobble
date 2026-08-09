use super::*;

impl ParquetIterator {
    pub(crate) fn new_with_columns(
        file: Box<dyn RandomAccessFile>,
        column_indices: Option<&[usize]>,
    ) -> Result<Self> {
        Self::new_with_ranges(
            file,
            None,
            None,
            None,
            ParquetIteratorOptions::default(),
            column_indices,
        )
    }
}
