use crate::data_file::DataFileType;
use crate::parquet::ParquetWriterOptions;
use crate::sst::SSTWriterOptions;

#[derive(Clone)]
pub(crate) enum WriterOptions {
    Sst(SSTWriterOptions),
    Parquet(ParquetWriterOptions),
}

#[derive(Clone)]
pub(crate) enum WriterOptionsFactory {
    Sst(SSTWriterOptions),
    Parquet(ParquetWriterOptions),
}

impl WriterOptionsFactory {
    pub(crate) fn build(&self, num_columns: usize, value_has_ttl: bool) -> WriterOptions {
        match self {
            WriterOptionsFactory::Sst(options) => {
                let mut options = options.clone();
                options.num_columns = num_columns;
                options.value_has_ttl = value_has_ttl;
                WriterOptions::Sst(options)
            }
            WriterOptionsFactory::Parquet(options) => {
                let mut options = options.clone();
                options.num_columns = num_columns;
                WriterOptions::Parquet(options)
            }
        }
    }

    pub(crate) fn data_file_type(&self) -> DataFileType {
        match self {
            WriterOptionsFactory::Sst(_) => DataFileType::SSTable,
            WriterOptionsFactory::Parquet(_) => DataFileType::Parquet,
        }
    }
}

impl From<&WriterOptions> for WriterOptionsFactory {
    fn from(options: &WriterOptions) -> Self {
        match options {
            WriterOptions::Sst(options) => WriterOptionsFactory::Sst(options.clone()),
            WriterOptions::Parquet(options) => WriterOptionsFactory::Parquet(options.clone()),
        }
    }
}

impl From<WriterOptions> for WriterOptionsFactory {
    fn from(options: WriterOptions) -> Self {
        match options {
            WriterOptions::Sst(options) => WriterOptionsFactory::Sst(options),
            WriterOptions::Parquet(options) => WriterOptionsFactory::Parquet(options),
        }
    }
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
