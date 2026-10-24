package io.cobble.table;

import java.io.Serializable;
import java.util.List;

/** Compiled row projection shared by native scans and format plugins. */
final class TableReadProjection implements Serializable {
    private static final long serialVersionUID = 1L;
    final TableReadSchema schema;
    final ValueProjection fields;

    TableReadProjection(TableReadSchema source, TableReadSchema output) {
        this.schema = output;
        this.fields =
                ValueProjection.compile(
                        LogicalTypes.struct(new RecordType(source.fields())),
                        LogicalTypes.struct(new RecordType(output.fields())));
    }

    static TableReadCursor<List<Value>> cursor(
            TableReadCursor<List<Value>> delegate, TableReadProjection projection) {
        return projection.fields.identity ? delegate : new Cursor(delegate, projection);
    }

    private static final class Cursor implements TableReadCursor<List<Value>> {
        private final TableReadCursor<List<Value>> delegate;
        private final TableReadProjection projection;

        private Cursor(TableReadCursor<List<Value>> delegate, TableReadProjection projection) {
            this.delegate = delegate;
            this.projection = projection;
        }

        @Override
        public TableReadEntry<List<Value>> next() throws Exception {
            TableReadEntry<List<Value>> entry = delegate.next();
            return entry == null ? null : project(entry, projection);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static TableReadEntry<List<Value>> project(
            TableReadEntry<List<Value>> entry, TableReadProjection projection) {
        List<Value> values = projection.fields.projectFields(entry.value());
        return new TableReadEntry<List<Value>>(
                entry.position(), values, entry.physicalBytes(), entry.countsPhysicalEntry());
    }
}
