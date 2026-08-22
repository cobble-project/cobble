package io.cobble.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Internal physical record mapping derived from a {@link TableSchema}. */
final class RecordLayout {
    private final int version;
    private final String codec;
    private final List<Long> keyFields;
    private final List<Long> bucketFields;
    private final List<ValueColumnLayout> valueColumns;
    private final String fingerprint;

    RecordLayout(
            int version,
            String codec,
            List<Long> keyFields,
            List<Long> bucketFields,
            List<ValueColumnLayout> valueColumns,
            String fingerprint) {
        this.version = version;
        this.codec = Objects.requireNonNull(codec, "codec");
        this.keyFields = immutableIds(keyFields);
        this.bucketFields = immutableIds(bucketFields);
        this.valueColumns =
                Collections.unmodifiableList(new ArrayList<ValueColumnLayout>(valueColumns));
        this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
    }

    int version() {
        return version;
    }

    String codec() {
        return codec;
    }

    List<Long> keyFields() {
        return keyFields;
    }

    List<Long> bucketFields() {
        return bucketFields;
    }

    List<ValueColumnLayout> valueColumns() {
        return valueColumns;
    }

    String fingerprint() {
        return fingerprint;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RecordLayout)) return false;
        RecordLayout that = (RecordLayout) other;
        return version == that.version
                && codec.equals(that.codec)
                && keyFields.equals(that.keyFields)
                && bucketFields.equals(that.bucketFields)
                && valueColumns.equals(that.valueColumns)
                && fingerprint.equals(that.fingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, codec, keyFields, bucketFields, valueColumns, fingerprint);
    }

    private static List<Long> immutableIds(List<Long> ids) {
        return Collections.unmodifiableList(new ArrayList<Long>(ids));
    }
}
