package io.cobble.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Neutral row layout metadata for a pluggable read session. */
public final class TableReadSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<DataField> fields;

    /**
     * Neutral typed output fields. Unlike {@link TableSchema}, this schema deliberately has no
     * primary-key requirement: formats such as list state may emit several logical rows from one
     * physical entry.
     */
    public TableReadSchema(List<DataField> fields) {
        Objects.requireNonNull(fields, "fields");
        this.fields = Collections.unmodifiableList(new ArrayList<DataField>(fields));
    }

    public List<DataField> fields() {
        return fields;
    }
}
