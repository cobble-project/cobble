package io.cobble.table;

import io.cobble.Config;

import java.io.Serializable;
import java.util.List;

/**
 * Shared immutable execution state transported with each standalone split, never its parent plan.
 */
final class TableScanReadContext implements Serializable {
    private static final long serialVersionUID = 1L;
    final TableReadProjection projection;
    final NativeTableScanProjection nativeProjection;
    private final TableReadSnapshot snapshot;
    private final TableReadCapabilities capabilities;

    TableScanReadContext(TableSchema source, TableReadProjection projection) {
        this.projection = projection;
        this.nativeProjection = new NativeTableScanProjection(source, projection);
        this.snapshot = null;
        this.capabilities = null;
    }

    TableScanReadContext(
            TableReadSnapshot snapshot,
            TableReadCapabilities capabilities,
            TableReadProjection projection) {
        this.projection = projection;
        this.nativeProjection = null;
        this.snapshot = snapshot;
        this.capabilities = capabilities;
    }

    TableScanReadContext project(TableSchema nativeSchema, TableReadProjection selected) {
        return nativeProjection != null
                ? new TableScanReadContext(nativeSchema, selected)
                : new TableScanReadContext(snapshot, capabilities, selected);
    }

    TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split) {
        if (nativeProjection != null) return new NativeTableScanReadProvider(config, split);
        return new PhysicalProvider(
                config, split, TableFormatPluginRegistry.resolve(split.formatId()));
    }

    private final class PhysicalProvider implements TableReadProvider<List<Value>, Void> {
        private final Config config;
        private final TableScanSplit split;
        private final TableFormatPlugin plugin;

        private PhysicalProvider(Config config, TableScanSplit split, TableFormatPlugin plugin) {
            this.config = config.copy();
            this.split = split;
            this.plugin = plugin;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return capabilities;
        }

        @Override
        public TableReadSession<List<Value>, Void> open() throws Exception {
            TableFormatBinding binding = plugin.bind(snapshot);
            return new SplitSession(new PhysicalTableReadSession(config, snapshot, binding), split);
        }

        @Override
        public void close() {}
    }

    private final class SplitSession implements TableReadSession<List<Value>, Void> {
        private final PhysicalTableReadSession delegate;
        private final TableScanSplit split;

        private SplitSession(PhysicalTableReadSession delegate, TableScanSplit split) {
            this.delegate = delegate;
            this.split = split;
        }

        @Override
        public TableReadSchema schema() {
            return projection.schema;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position)
                throws Exception {
            return TableReadProjection.cursor(
                    delegate.scan(range, position, split.shardSnapshot().ranges), projection);
        }

        @Override
        public java.util.Collection<TableReadEntry<List<Value>>> lookup(Void unused) {
            throw new UnsupportedOperationException("scan assignments do not support lookup");
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
