package io.cobble.table;

import io.cobble.Config;

import java.util.List;

/** Built-in adapter for snapshots written by Cobble's native typed table. */
public final class NativeTableReadFormatFactory implements TableReadFormatFactory {
    @Override
    public String formatId() {
        return TableMetadata.FORMAT;
    }

    @Override
    public TableScanPlan plan(Config config, TableReadSnapshot snapshot) {
        if (snapshot.globalSnapshot() == null) {
            throw new IllegalArgumentException(
                    "native table scans require a fixed global snapshot");
        }
        if (!formatId().equals(snapshot.formatId())) {
            throw new IllegalArgumentException(
                    "native table factory cannot read format '" + snapshot.formatId() + "'");
        }
        return TableScanPlan.nativeForSnapshot(
                config, snapshot.columnFamily(), snapshot.globalSnapshot().id);
    }

    @Override
    public TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split) {
        if (!formatId().equals(split.formatId())) {
            throw new IllegalArgumentException(
                    "native table split has unexpected format '" + split.formatId() + "'");
        }
        return new NativeTableScanReadProvider(config, split);
    }
}
