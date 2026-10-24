package io.cobble.table;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A compiled, immutable selection of a value's nested fields, without type conversion. */
public final class ValueProjection implements Serializable {
    private static final long serialVersionUID = 1L;
    final LogicalType sourceType;
    final boolean identity;
    final int[] indexes;
    final int[] sourceToOutput;
    final ValueProjection[] children;

    private ValueProjection(
            LogicalType source, LogicalType output, int[] indexes, ValueProjection[] children) {
        this.sourceType = source;
        this.identity = source.equals(output);
        this.indexes = indexes;
        this.children = children;
        this.sourceToOutput =
                indexes == null
                        ? null
                        : new int[((StructType) source).recordType().fields().size()];
        if (sourceToOutput != null) {
            Arrays.fill(sourceToOutput, -1);
            for (int i = 0; i < indexes.length; i++) sourceToOutput[indexes[i]] = i;
        }
    }

    /**
     * Compiles struct fields by stable ID; list elements and map values may be projected. This is
     * selection, not schema evolution: leaf types, nullability and map keys must remain unchanged.
     * Extension values are opaque and can only be selected whole.
     */
    public static ValueProjection compile(LogicalType source, LogicalType output) {
        Objects.requireNonNull(source, "sourceType");
        Objects.requireNonNull(output, "outputType");
        if (source.equals(output)) return new ValueProjection(source, output, null, null);
        if (source.kind() != output.kind() || source.isNullable() != output.isNullable())
            throw new IllegalArgumentException(
                    "projection must preserve value type and nullability");
        switch (source.kind()) {
            case STRUCT:
                List<DataField> sourceFields = ((StructType) source).recordType().fields();
                List<DataField> outputFields = ((StructType) output).recordType().fields();
                int[] indexes = new int[outputFields.size()];
                ValueProjection[] children = new ValueProjection[indexes.length];
                for (int i = 0; i < indexes.length; i++) {
                    DataField requested = outputFields.get(i);
                    int index = -1;
                    for (int j = 0; j < sourceFields.size(); j++)
                        if (sourceFields.get(j).id() == requested.id()) index = j;
                    if (index < 0)
                        throw new IllegalArgumentException(
                                "unknown projected field ID: " + requested.id());
                    indexes[i] = index;
                    children[i] =
                            compile(sourceFields.get(index).logicalType(), requested.logicalType());
                }
                return new ValueProjection(source, output, indexes, children);
            case LIST:
                return new ValueProjection(
                        source,
                        output,
                        null,
                        new ValueProjection[] {
                            compile(
                                    ((ListType) source).elementType(),
                                    ((ListType) output).elementType())
                        });
            case MAP:
                MapType sourceMap = (MapType) source;
                MapType outputMap = (MapType) output;
                if (!sourceMap.keyType().equals(outputMap.keyType()))
                    throw new IllegalArgumentException("projection must preserve map keys");
                return new ValueProjection(
                        source,
                        output,
                        null,
                        new ValueProjection[] {
                            compile(sourceMap.valueType(), outputMap.valueType())
                        });
            default:
                throw new IllegalArgumentException("projection must preserve leaf types");
        }
    }

    /** Decodes only selected fields; binary leaves retain the codec's borrowed-buffer semantics. */
    public Value decode(ByteBuffer input) {
        return ValueProjectionDecoder.decode(this, input, false);
    }

    /** Decodes only selected fields and copies selected binary leaves out of the input buffer. */
    public Value decodeOwned(ByteBuffer input) {
        return ValueProjectionDecoder.decode(this, input, true);
    }

    /** Applies the same selection to an already decoded value (for format plugins). */
    public Value project(Value value) {
        if (identity || value.kind() == Value.Kind.NULL) return value;
        switch (sourceType.kind()) {
            case STRUCT:
                return Value.struct(projectFields(value.cast()));
            case LIST:
                List<Value> elements = value.cast();
                List<Value> selected = new ArrayList<Value>(elements.size());
                for (Value element : elements) selected.add(children[0].project(element));
                return Value.list(selected);
            case MAP:
                List<Map.Entry<Value, Value>> entries = value.cast();
                List<Map.Entry<Value, Value>> result =
                        new ArrayList<Map.Entry<Value, Value>>(entries.size());
                for (Map.Entry<Value, Value> entry : entries)
                    result.add(
                            new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                    entry.getKey(), children[0].project(entry.getValue())));
                return Value.map(result);
            default:
                throw new AssertionError(sourceType.kind());
        }
    }

    List<Value> projectFields(List<Value> values) {
        if (identity) return values;
        List<Value> selected = new ArrayList<Value>(indexes.length);
        for (int i = 0; i < indexes.length; i++)
            selected.add(children[i].project(values.get(indexes[i])));
        return selected;
    }
}
