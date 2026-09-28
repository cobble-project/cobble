package io.cobble.table;

import io.cobble.NativeLoader;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** One immutable catalog schema-history value, including its stable field identities. */
public final class CatalogSchemaVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String versionJson;
    private final transient long tableId;
    private final transient long catalogSchemaId;
    private final transient TableSchema schema;

    private CatalogSchemaVersion(String versionJson) {
        this.versionJson = Objects.requireNonNull(versionJson, "versionJson");
        JsonObject value = JsonParser.parseString(versionJson).getAsJsonObject();
        tableId = value.get("table_id").getAsLong();
        catalogSchemaId = value.get("catalog_schema_id").getAsLong();
        schema = TableSchema.fromJson(value.getAsJsonObject("schema").toString());
    }

    /** Creates an unpublished initial version for an identity allocated by the backend. */
    public static CatalogSchemaVersion initial(long tableId, TableSchema schema) {
        Objects.requireNonNull(schema, "schema");
        NativeLoader.load();
        return fromNativeJson(initialNative(tableId, schema.toJson()));
    }

    /** Computes an unpublished successor; the backend coordinates and persists its publication. */
    public CatalogSchemaVersion evolve(List<TableSchemaChange> changes) {
        NativeLoader.load();
        return fromNativeJson(evolveNative(versionJson, TableSchemaChange.toJson(changes)));
    }

    public long tableId() {
        return tableId;
    }

    public long catalogSchemaId() {
        return catalogSchemaId;
    }

    public TableSchema schema() {
        return schema;
    }

    String nativeJson() {
        return versionJson;
    }

    static CatalogSchemaVersion fromNativeJson(String json) {
        if (json == null || json.trim().isEmpty())
            throw new IllegalStateException("failed to load catalog schema version");
        return new CatalogSchemaVersion(json);
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            return fromNativeJson(versionJson);
        } catch (RuntimeException e) {
            InvalidObjectException invalid =
                    new InvalidObjectException("invalid catalog schema version");
            invalid.initCause(e);
            throw invalid;
        }
    }

    private static native String initialNative(long tableId, String schemaJson);

    private static native String evolveNative(String versionJson, String changesJson);
}
