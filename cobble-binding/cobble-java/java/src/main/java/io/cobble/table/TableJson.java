package io.cobble.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Package-private JSON bridge matching Rust's serde field names and logical-type shape. */
final class TableJson {
    private TableJson() {}

    static String toJson(TableSchema schema) {
        return schemaObject(schema).toString();
    }

    static String toJson(TableMetadata metadata) {
        return metadataObject(metadata).toString();
    }

    static TableSchema schemaFromJson(String json) {
        return schemaFromObject(object(json));
    }

    static TableMetadata metadataFromJson(String json) {
        return metadataFromObject(object(json));
    }

    private static JsonObject schemaObject(TableSchema schema) {
        JsonObject object = new JsonObject();
        object.add("fields", fieldsObject(schema.fields()));
        object.add("primary_key", idsObject(schema.primaryKey()));
        object.add("bucket_key", idsObject(schema.bucketKey()));
        return object;
    }

    private static TableSchema schemaFromObject(JsonObject object) {
        return new TableSchema(
                fieldsFromObject(requiredArray(object, "fields")),
                idsFromObject(requiredArray(object, "primary_key")),
                idsFromObject(requiredArray(object, "bucket_key")));
    }

    private static JsonObject metadataObject(TableMetadata metadata) {
        JsonObject object = new JsonObject();
        object.addProperty("format", metadata.format());
        object.addProperty("version", metadata.version());
        object.add("schema", schemaObject(metadata.schema()));
        object.add("layout", layoutObject(metadata.layout()));
        return object;
    }

    private static TableMetadata metadataFromObject(JsonObject object) {
        return new TableMetadata(
                requiredString(object, "format"),
                requiredInt(object, "version"),
                schemaFromObject(requiredObject(object, "schema")),
                layoutFromObject(requiredObject(object, "layout")));
    }

    private static JsonObject layoutObject(RecordLayout layout) {
        JsonObject object = new JsonObject();
        object.addProperty("version", layout.version());
        object.addProperty("codec", layout.codec());
        object.add("key_fields", idsObject(layout.keyFields()));
        object.add("bucket_fields", idsObject(layout.bucketFields()));
        JsonArray columns = new JsonArray();
        for (ValueColumnLayout column : layout.valueColumns()) {
            JsonObject value = new JsonObject();
            value.addProperty("field_id", column.fieldId());
            value.addProperty("column_index", column.columnIndex());
            value.addProperty("storage", snake(column.storage().name()));
            columns.add(value);
        }
        object.add("value_columns", columns);
        object.addProperty("fingerprint", layout.fingerprint());
        return object;
    }

    private static RecordLayout layoutFromObject(JsonObject object) {
        List<ValueColumnLayout> columns = new ArrayList<ValueColumnLayout>();
        for (JsonElement value : requiredArray(object, "value_columns")) {
            JsonObject column = value.getAsJsonObject();
            columns.add(
                    new ValueColumnLayout(
                            requiredLong(column, "field_id"),
                            requiredInt(column, "column_index"),
                            ValueStorage.valueOf(
                                    requiredString(column, "storage").toUpperCase(Locale.ROOT))));
        }
        return new RecordLayout(
                requiredInt(object, "version"),
                requiredString(object, "codec"),
                idsFromObject(requiredArray(object, "key_fields")),
                idsFromObject(requiredArray(object, "bucket_fields")),
                columns,
                requiredString(object, "fingerprint"));
    }

    private static JsonArray fieldsObject(List<DataField> fields) {
        JsonArray values = new JsonArray();
        for (DataField field : fields) values.add(fieldObject(field));
        return values;
    }

    private static List<DataField> fieldsFromObject(JsonArray fields) {
        List<DataField> values = new ArrayList<DataField>();
        for (JsonElement value : fields) values.add(fieldFromObject(value.getAsJsonObject()));
        return values;
    }

    private static JsonObject fieldObject(DataField field) {
        JsonObject object = new JsonObject();
        object.addProperty("id", field.id());
        object.addProperty("name", field.name());
        object.add("logical_type", typeObject(field.logicalType()));
        return object;
    }

    private static DataField fieldFromObject(JsonObject object) {
        return new DataField(
                requiredLong(object, "id"),
                requiredString(object, "name"),
                typeFromObject(requiredObject(object, "logical_type")));
    }

    private static JsonObject typeObject(LogicalType type) {
        JsonObject object = new JsonObject();
        object.addProperty("nullable", type.isNullable());
        object.addProperty("kind", snake(type.kind().name()));
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            object.addProperty("precision", decimal.precision());
            object.addProperty("scale", decimal.scale());
        } else if (type instanceof TimeType) {
            object.addProperty("precision", ((TimeType) type).precision());
        } else if (type instanceof TimestampType) {
            TimestampType timestamp = (TimestampType) type;
            object.addProperty("precision", timestamp.precision());
            object.addProperty("timestamp_kind", snake(timestamp.timestampKind().name()));
        } else if (type instanceof ListType) {
            object.add("element_type", typeObject(((ListType) type).elementType()));
        } else if (type instanceof MapType) {
            MapType map = (MapType) type;
            object.add("key_type", typeObject(map.keyType()));
            object.add("value_type", typeObject(map.valueType()));
        } else if (type instanceof StructType) {
            object.add("fields", fieldsObject(((StructType) type).recordType().fields()));
        } else if (type instanceof ExtensionType) {
            ExtensionType extension = (ExtensionType) type;
            JsonObject extensionObject = new JsonObject();
            extensionObject.addProperty("type_id", extension.typeId());
            extensionObject.add("parameters", JsonParser.parseString(extension.parametersJson()));
            extensionObject.add("physical_type", typeObject(extension.physicalType()));
            object.add("extension", extensionObject);
        }
        return object;
    }

    private static LogicalType typeFromObject(JsonObject object) {
        boolean nullable = requiredBoolean(object, "nullable");
        String kind = requiredString(object, "kind");
        if ("boolean".equals(kind)) return LogicalTypes.booleanType(nullable);
        if ("int8".equals(kind)) return LogicalTypes.int8(nullable);
        if ("int16".equals(kind)) return LogicalTypes.int16(nullable);
        if ("int32".equals(kind)) return LogicalTypes.int32(nullable);
        if ("int64".equals(kind)) return LogicalTypes.int64(nullable);
        if ("float32".equals(kind)) return LogicalTypes.float32(nullable);
        if ("float64".equals(kind)) return LogicalTypes.float64(nullable);
        if ("date".equals(kind)) return LogicalTypes.date(nullable);
        if ("string".equals(kind)) return LogicalTypes.string(nullable);
        if ("binary".equals(kind)) return LogicalTypes.binary(nullable);
        if ("decimal".equals(kind))
            return new DecimalType(
                    requiredInt(object, "precision"), requiredInt(object, "scale"), nullable);
        if ("time".equals(kind)) return new TimeType(requiredInt(object, "precision"), nullable);
        if ("timestamp".equals(kind))
            return new TimestampType(
                    requiredInt(object, "precision"),
                    TimestampKind.valueOf(
                            requiredString(object, "timestamp_kind").toUpperCase(Locale.ROOT)),
                    nullable);
        if ("list".equals(kind))
            return new ListType(typeFromObject(requiredObject(object, "element_type")), nullable);
        if ("map".equals(kind))
            return new MapType(
                    typeFromObject(requiredObject(object, "key_type")),
                    typeFromObject(requiredObject(object, "value_type")),
                    nullable);
        if ("struct".equals(kind))
            return new StructType(
                    new RecordType(fieldsFromObject(requiredArray(object, "fields"))), nullable);
        if ("extension".equals(kind)) {
            JsonObject extension = requiredObject(object, "extension");
            return new ExtensionType(
                    requiredString(extension, "type_id"),
                    requiredObjectOrValue(extension, "parameters").toString(),
                    typeFromObject(requiredObject(extension, "physical_type")),
                    nullable);
        }
        throw new IllegalArgumentException("unsupported logical type: " + kind);
    }

    private static JsonArray idsObject(List<Long> ids) {
        JsonArray values = new JsonArray();
        for (Long id : ids) values.add(id);
        return values;
    }

    private static List<Long> idsFromObject(JsonArray ids) {
        List<Long> values = new ArrayList<Long>();
        for (JsonElement id : ids) values.add(id.getAsLong());
        return values;
    }

    private static JsonObject object(String json) {
        try {
            return JsonParser.parseString(json).getAsJsonObject();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid table JSON", e);
        }
    }

    private static JsonElement requiredObjectOrValue(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null) throw new IllegalArgumentException("missing JSON field: " + name);
        return value;
    }

    private static JsonObject requiredObject(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsJsonObject();
    }

    private static JsonArray requiredArray(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsJsonArray();
    }

    private static String requiredString(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsString();
    }

    private static long requiredLong(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsLong();
    }

    private static int requiredInt(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsInt();
    }

    private static boolean requiredBoolean(JsonObject object, String name) {
        return requiredObjectOrValue(object, name).getAsBoolean();
    }

    private static String snake(String value) {
        return value.toLowerCase(Locale.ROOT);
    }
}
