use crate::data_file::DataFileType;
use crate::parquet::ParquetWriterOptions;
use crate::sst::SSTWriterOptions;

#[derive(Clone)]
pub(crate) enum WriterOptions {
    Sst(SSTWriterOptions),
    Parquet(ParquetWriterOptions),
}

impl WriterOptions {
    pub(crate) fn data_file_type(&self) -> DataFileType {
        match self {
            WriterOptions::Sst(_) => DataFileType::SSTable,
            WriterOptions::Parquet(_) => DataFileType::Parquet,
        }
    }

    pub(crate) fn buffer_size(&self) -> usize {
        match self {
            WriterOptions::Sst(options) => options.buffer_size,
            WriterOptions::Parquet(options) => options.buffer_size,
        }
    }
}
