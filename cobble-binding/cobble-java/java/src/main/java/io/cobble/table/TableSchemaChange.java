package io.cobble.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;
import java.util.Objects;

/** One supported name-based catalog table schema edit. */
public final class TableSchemaChange {
    private final JsonObject value;

    private TableSchemaChange(String kind, JsonObject fields) {
        value = new JsonObject();
        value.add(kind, fields);
    }

    /** Append a nullable top-level value field. */
    public static TableSchemaChange addField(String name, LogicalType logicalType) {
        JsonObject fields = new JsonObject();
        fields.addProperty("name", Objects.requireNonNull(name, "name"));
        fields.add(
                "logical_type",
                TableJson.typeObject(Objects.requireNonNull(logicalType, "logicalType")));
        return new TableSchemaChange("AddField", fields);
    }

    /** Remove one non-key top-level field. */
    public static TableSchemaChange dropField(String fieldName) {
        JsonObject fields = new JsonObject();
        fields.addProperty("field_name", Objects.requireNonNull(fieldName, "fieldName"));
        return new TableSchemaChange("DropField", fields);
    }

    /** Rename one top-level field without changing its stable field identity. */
    public static TableSchemaChange renameField(String fieldName, String newName) {
        JsonObject fields = new JsonObject();
        fields.addProperty("field_name", Objects.requireNonNull(fieldName, "fieldName"));
        fields.addProperty("new_name", Objects.requireNonNull(newName, "newName"));
        return new TableSchemaChange("RenameField", fields);
    }

    /** Losslessly widen one non-key top-level field using Cobble's built-in table transform. */
    public static TableSchemaChange alterFieldType(String fieldName, LogicalType logicalType) {
        JsonObject fields = new JsonObject();
        fields.addProperty("field_name", Objects.requireNonNull(fieldName, "fieldName"));
        fields.add(
                "logical_type",
                TableJson.typeObject(Objects.requireNonNull(logicalType, "logicalType")));
        return new TableSchemaChange("AlterFieldType", fields);
    }

    static String toJson(List<TableSchemaChange> changes) {
        Objects.requireNonNull(changes, "changes");
        JsonArray values = new JsonArray();
        for (TableSchemaChange change : changes)
            values.add(Objects.requireNonNull(change, "change").value);
        return values.toString();
    }
}
