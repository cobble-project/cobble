package io.cobble.table;

import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Immutable native column selection and owned row decoder, compiled before split distribution. */
final class NativeTableScanProjection implements Serializable {
    private static final long serialVersionUID = 1L;
    final TableReadSchema schema;
    final List<String> fieldNames;
    private final List<LogicalType> keyTypes;
    private final int[] keyIndexes;
    private final int[] valueIndexes;
    private final ValueProjection[] valueProjections;
    private final boolean hasKey;
    private final boolean hasValues;

    NativeTableScanProjection(TableSchema source, TableReadProjection projection) {
        this.schema = projection.schema;
        List<LogicalType> keys = new ArrayList<LogicalType>(source.primaryKey().size());
        for (long id : source.primaryKey()) {
            for (DataField field : source.fields()) {
                if (field.id() == id) keys.add(field.logicalType());
            }
        }
        this.keyTypes = Collections.unmodifiableList(keys);
        int size = projection.schema.fields().size();
        this.keyIndexes = new int[size];
        this.valueIndexes = new int[size];
        this.valueProjections = new ValueProjection[size];
        Arrays.fill(keyIndexes, -1);
        Arrays.fill(valueIndexes, -1);
        List<String> names = new ArrayList<String>(size);
        int valueCount = 0;
        boolean keySelected = false;
        for (int i = 0; i < size; i++) {
            int index = projection.fields.identity ? i : projection.fields.indexes[i];
            DataField field = source.fields().get(index);
            names.add(field.name());
            int keyIndex = source.primaryKey().indexOf(field.id());
            if (keyIndex >= 0) {
                keyIndexes[i] = keyIndex;
                keySelected = true;
            } else {
                valueIndexes[i] = valueCount++;
                // An identity projection uses the ordinary codec; do not compile children again.
                valueProjections[i] =
                        projection.fields.identity ? null : projection.fields.children[i];
            }
        }
        // Request a key for empty logical rows, preserving the native row-existence anchor.
        if (names.isEmpty()) {
            long keyId = source.primaryKey().get(0);
            for (DataField field : source.fields()) {
                if (field.id() == keyId) names.add(field.name());
            }
        }
        this.fieldNames = Collections.unmodifiableList(names);
        this.hasKey = keySelected;
        this.hasValues = valueCount > 0;
    }

    List<Value> decode(DirectScanEntry entry) {
        if (keyIndexes.length == 0) return Collections.emptyList();
        List<Value> keys = hasKey ? KeyCodec.decodeOwned(keyTypes, entry.getKey()) : null;
        DirectColumns columns = hasValues ? entry.columnsView() : null;
        ArrayList<Value> row = new ArrayList<Value>(keyIndexes.length);
        for (int i = 0; i < keyIndexes.length; i++) {
            if (keyIndexes[i] >= 0) {
                row.add(keys.get(keyIndexes[i]));
            } else {
                int columnIndex = valueIndexes[i];
                if (columnIndex >= columns.size()) {
                    throw new IllegalStateException("table row has an incompatible projection");
                }
                ByteBuffer value = columns.get(columnIndex);
                LogicalType type = schema.fields().get(i).logicalType();
                if (value == null) {
                    if (!type.isNullable())
                        throw new IllegalStateException("table row is missing a value column");
                    row.add(Value.nullValue());
                } else {
                    ValueProjection decoder = valueProjections[i];
                    row.add(
                            decoder == null
                                    ? ValueCodec.decodeOwned(type, value)
                                    : decoder.decodeOwned(value));
                }
            }
        }
        return Collections.unmodifiableList(row);
    }
}
