package io.cobble.table;

import java.util.Collections;
import java.util.List;

/** A validated and encoded primary key for a table. */
public final class TableKey {
    private final List<Value> values;
    private final int bucket;
    private final byte[] encoded;

    TableKey(List<Value> values, int bucket, byte[] encoded) {
        this.values = Collections.unmodifiableList(values);
        this.bucket = bucket;
        this.encoded = encoded;
    }

    /** Returns the bucket selected for this key. */
    public int bucket() {
        return bucket;
    }

    List<Value> valuesInternal() {
        return values;
    }

    byte[] encodedInternal() {
        return encoded;
    }
}
