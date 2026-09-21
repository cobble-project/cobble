package io.cobble.table;

import io.cobble.Config;

import java.util.List;

/** Service-loaded implementation for one exact snapshot-embedded table format. */
public interface TableReadFormatFactory {
    /** Exact {@code format} value stored in the selected column family's snapshot metadata. */
    String formatId();

    /** Creates a fixed, serializable scan plan from hydrated snapshot metadata. */
    TableScanPlan plan(Config config, TableReadSnapshot snapshot) throws Exception;

    /** Opens one split on the worker using only the split's persisted format selection. */
    TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split) throws Exception;
}
