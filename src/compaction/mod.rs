//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

mod executor;
mod file_builder;

#[allow(unused_imports)]
pub(crate) use executor::{
    CompactionExecutor, CompactionOptions, CompactionResult, CompactionTask,
};

pub(crate) use file_builder::{FileBuilder, FileBuilderFactory};
