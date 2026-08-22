package io.cobble.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** User-visible semantic schema of a table. */
public final class TableSchema {
    private final List<DataField> fields;
    private final List<Long> primaryKey;
    private final List<Long> bucketKey;

    public TableSchema(List<DataField> fields, List<Long> primaryKey, List<Long> bucketKey) {
        this.fields = Collections.unmodifiableList(new ArrayList<DataField>(fields));
        this.primaryKey = immutableFieldIds(primaryKey, "primaryKey");
        this.bucketKey = immutableFieldIds(bucketKey, "bucketKey");
        validate();
    }

    public List<DataField> fields() {
        return fields;
    }

    public List<Long> primaryKey() {
        return primaryKey;
    }

    public List<Long> bucketKey() {
        return bucketKey;
    }

    String toJson() {
        return TableJson.toJson(this);
    }

    static TableSchema fromJson(String json) {
        return TableJson.schemaFromJson(json);
    }

    private void validate() {
        if (fields.isEmpty() || primaryKey.isEmpty() || bucketKey.isEmpty())
            throw new IllegalArgumentException(
                    "table schema requires fields, primary key, and bucket key");
        if (bucketKey.size() > primaryKey.size()
                || !primaryKey.subList(0, bucketKey.size()).equals(bucketKey))
            throw new IllegalArgumentException("bucket key must be a primary-key prefix");

        Map<Long, DataField> topLevel = new HashMap<Long, DataField>();
        Set<Long> allIds = new HashSet<Long>();
        Set<String> topLevelNames = new HashSet<String>();
        for (DataField field : fields) {
            if (!topLevelNames.add(field.name()) || topLevel.put(field.id(), field) != null)
                throw new IllegalArgumentException("duplicate top-level table field");
            validateField(field, allIds);
        }
        Set<Long> keyIds = new HashSet<Long>();
        for (Long id : primaryKey) {
            DataField field = topLevel.get(id);
            if (field == null || !keyIds.add(id) || !field.logicalType().isKeyCompatible())
                throw new IllegalArgumentException("invalid primary key field: " + id);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TableSchema)) return false;
        TableSchema that = (TableSchema) other;
        return fields.equals(that.fields)
                && primaryKey.equals(that.primaryKey)
                && bucketKey.equals(that.bucketKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, primaryKey, bucketKey);
    }

    private static List<Long> immutableFieldIds(List<Long> ids, String name) {
        Objects.requireNonNull(ids, name);
        ArrayList<Long> copied = new ArrayList<Long>(ids.size());
        for (Long id : ids) {
            if (id == null || id < 0 || id > 0xffffffffL)
                throw new IllegalArgumentException("invalid field id in " + name);
            copied.add(id);
        }
        return Collections.unmodifiableList(copied);
    }

    private static void validateField(DataField field, Set<Long> ids) {
        if (!ids.add(field.id())) throw new IllegalArgumentException("duplicate table field id");
        LogicalType type = field.logicalType();
        type.validate();
        if (type instanceof ListType) {
            validateNestedType(((ListType) type).elementType(), ids);
        } else if (type instanceof MapType) {
            validateNestedType(((MapType) type).keyType(), ids);
            validateNestedType(((MapType) type).valueType(), ids);
        } else if (type instanceof StructType) {
            validateRecordFields(((StructType) type).recordType(), ids);
        } else if (type instanceof ExtensionType) {
            validateNestedType(((ExtensionType) type).physicalType(), ids);
        }
    }

    private static void validateNestedType(LogicalType type, Set<Long> ids) {
        type.validate();
        if (type instanceof ListType) {
            validateNestedType(((ListType) type).elementType(), ids);
        } else if (type instanceof MapType) {
            validateNestedType(((MapType) type).keyType(), ids);
            validateNestedType(((MapType) type).valueType(), ids);
        } else if (type instanceof StructType) {
            validateRecordFields(((StructType) type).recordType(), ids);
        } else if (type instanceof ExtensionType) {
            validateNestedType(((ExtensionType) type).physicalType(), ids);
        }
    }

    private static void validateRecordFields(RecordType recordType, Set<Long> ids) {
        Set<String> names = new HashSet<String>();
        for (DataField field : recordType.fields()) {
            if (!names.add(field.name()))
                throw new IllegalArgumentException("duplicate nested field");
            validateField(field, ids);
        }
    }
}
