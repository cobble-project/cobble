package io.cobble.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Incrementally builds one table primary key in schema order. */
public final class TableKeyBuilder {
    private final Table.Compiled compiled;
    private final List<Value> values;

    TableKeyBuilder(Table.Compiled compiled) {
        this.compiled = compiled;
        this.values = new ArrayList<Value>(compiled.keyTypes.size());
    }

    /** Appends the next primary-key field. */
    public TableKeyBuilder push(Value value) {
        values.add(Objects.requireNonNull(value, "value"));
        return this;
    }

    /** Validates and encodes the complete primary key. */
    public TableKey build() {
        if (values.size() != compiled.keyTypes.size())
            throw new IllegalArgumentException("primary key field count does not match schema");
        List<Value> stableValues = stableValues();
        ByteBuffer encoded =
                ByteBuffer.allocate(KeyCodec.encodedSize(compiled.keyTypes, stableValues));
        int prefixEnd =
                KeyCodec.encodeToWithPrefix(
                        compiled.keyTypes, stableValues, compiled.bucketKeyFields, encoded);
        int bucket = compiled.bucketHash.bucket(ByteBuffer.wrap(encoded.array(), 0, prefixEnd));
        return new TableKey(stableValues, bucket, encoded.array());
    }

    private List<Value> stableValues() {
        List<Value> stable = new ArrayList<Value>(values.size());
        for (Value value : values) {
            if (value != null && value.kind() == Value.Kind.BINARY) {
                ByteBuffer binary = (ByteBuffer) value.raw();
                byte[] copy = new byte[binary.remaining()];
                binary.get(copy);
                stable.add(Value.binary(ByteBuffer.wrap(copy)));
            } else {
                stable.add(value);
            }
        }
        return stable;
    }
}
