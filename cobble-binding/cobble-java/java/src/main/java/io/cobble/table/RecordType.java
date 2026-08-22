package io.cobble.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Ordered fields of a nested struct. */
public final class RecordType {
    private final List<DataField> fields;

    public RecordType(List<DataField> fields) {
        this.fields = Collections.unmodifiableList(new ArrayList<DataField>(fields));
        validate();
    }

    public List<DataField> fields() {
        return fields;
    }

    public void validate() {
        Set<Long> ids = new HashSet<Long>();
        Set<String> names = new HashSet<String>();
        for (DataField field : fields) {
            if (!ids.add(field.id()) || !names.add(field.name()))
                throw new IllegalArgumentException("duplicate struct field");
            field.logicalType().validate();
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RecordType && fields.equals(((RecordType) other).fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
