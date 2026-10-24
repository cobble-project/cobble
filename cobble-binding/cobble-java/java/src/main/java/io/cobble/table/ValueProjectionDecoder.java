package io.cobble.table;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Selective container traversal sharing the value codec's scalar and framing primitives. */
final class ValueProjectionDecoder {
    private ValueProjectionDecoder() {}

    static Value decode(ValueProjection projection, ByteBuffer input, boolean owned) {
        if (projection.identity)
            return owned
                    ? ValueCodec.decodeOwned(projection.sourceType, input)
                    : ValueCodec.decode(projection.sourceType, input);
        if (!ValueCodec.readNonNull(projection.sourceType, input)) return Value.nullValue();
        Value value;
        switch (projection.sourceType.kind()) {
            case STRUCT:
                Value[] fields = new Value[projection.indexes.length];
                for (int source = 0; source < projection.sourceToOutput.length; source++) {
                    int selected = projection.sourceToOutput[source];
                    if (selected < 0) ValueCodec.skipFrame(input);
                    else
                        fields[selected] =
                                decode(
                                        projection.children[selected],
                                        ValueCodec.readFrame(input),
                                        owned);
                }
                value = Value.struct(Arrays.asList(fields));
                break;
            case LIST:
                int elements = ValueCodec.getCount(input);
                if (elements > input.remaining() / 4)
                    throw new IllegalArgumentException("list count exceeds encoded frames");
                List<Value> list = new ArrayList<Value>(elements);
                for (int i = 0; i < elements; i++)
                    list.add(decode(projection.children[0], ValueCodec.readFrame(input), owned));
                value = Value.list(list);
                break;
            case MAP:
                int size = ValueCodec.getCount(input);
                if (size > input.remaining() / 8)
                    throw new IllegalArgumentException("map count exceeds encoded frames");
                List<Map.Entry<Value, Value>> entries =
                        new ArrayList<Map.Entry<Value, Value>>(size);
                LogicalType keyType = ((MapType) projection.sourceType).keyType();
                for (int i = 0; i < size; i++) {
                    ByteBuffer keyBytes = ValueCodec.readFrame(input);
                    Value key =
                            owned
                                    ? ValueCodec.decodeOwned(keyType, keyBytes)
                                    : ValueCodec.decode(keyType, keyBytes);
                    entries.add(
                            new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                    key,
                                    decode(
                                            projection.children[0],
                                            ValueCodec.readFrame(input),
                                            owned)));
                }
                value = Value.map(entries);
                break;
            default:
                throw new AssertionError(projection.sourceType.kind());
        }
        return ValueCodec.finish(input, value);
    }
}
