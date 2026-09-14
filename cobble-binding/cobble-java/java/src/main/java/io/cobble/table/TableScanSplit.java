package io.cobble.table;

import io.cobble.Config;
import io.cobble.DirectScanCursor;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.ScanCursor;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Serializable table-aware scan assignment for one shard snapshot. */
public final class TableScanSplit implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String splitJson;
    private transient TableSchema schema;
    private transient int totalBuckets;

    TableScanSplit(String splitJson) {
        if (splitJson == null || splitJson.trim().isEmpty()) {
            throw new IllegalArgumentException("splitJson must not be empty");
        }
        this.splitJson = splitJson;
        loadMetadata();
    }

    /**
     * Open an encoded scan cursor for selected semantic fields.
     *
     * <p>The cursor returns the encoded primary key and only the physical value columns needed by
     * non-key {@code fieldNames}, in requested order. Key fields are always encoded in {@link
     * ScanCursor.Entry#key}; a key-only projection retains physical column zero for row existence
     * and callers should ignore {@link ScanCursor.Entry#columns}. Built-in persisted table schema
     * transforms are applied before rows are returned.
     */
    public ScanCursor openScanner(Config config, List<String> fieldNames, int readAheadBytes) {
        return (ScanCursor) openScanner(config, fieldNames, readAheadBytes, false);
    }

    /**
     * Open a direct encoded scan cursor for selected semantic fields.
     *
     * <p>The projected columns and row-existence semantics match {@link #openScanner(Config, List,
     * int)}, while entry buffers are exposed by {@link DirectScanCursor} rather than copied into
     * byte arrays.
     */
    public DirectScanCursor openDirectScanner(
            Config config, List<String> fieldNames, int readAheadBytes) {
        return (DirectScanCursor) openScanner(config, fieldNames, readAheadBytes, true);
    }

    private NativeObject openScanner(
            Config config, List<String> fieldNames, int readAheadBytes, boolean direct) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("fieldNames must not be empty");
        }
        if (readAheadBytes < 0) {
            throw new IllegalArgumentException("readAheadBytes must be >= 0");
        }
        String[] names = fieldNames.toArray(new String[0]);
        Set<String> seen = new HashSet<String>();
        Set<String> schemaFields = new HashSet<String>();
        for (DataField field : schema.fields()) schemaFields.add(field.name());
        for (String name : names) {
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("fieldNames must not contain empty names");
            }
            if (!seen.add(name)) {
                throw new IllegalArgumentException("fieldNames must not contain duplicate names");
            }
            if (!schemaFields.contains(name)) {
                throw new IllegalArgumentException("fieldNames contains no table field: " + name);
            }
        }
        NativeLoader.load();
        NativeObject cursor =
                openScannerNative(config.toJson(), splitJson, names, readAheadBytes, direct);
        if (cursor == null) {
            throw new IllegalStateException("failed to open table scan cursor");
        }
        return cursor;
    }

    /**
     * Open a typed full-row scan using this split's fixed schema.
     *
     * <p>Decodes directly from a reusable native I/O buffer without intermediate byte arrays.
     * Returned rows own their data and remain valid after the cursor advances or closes.
     *
     * <p>This requests every semantic field, so it is intentionally distinct from {@link
     * #openScanner(Config, List, int)}, which retains the raw projected connector path.
     */
    public TableScanCursor openTypedScanner(Config config, int readAheadBytes) {
        List<String> fieldNames = new ArrayList<String>(schema.fields().size());
        for (DataField field : schema.fields()) fieldNames.add(field.name());
        Table.Compiled compiled = Table.Compiled.from(schema, totalBuckets);
        DirectScanCursor cursor = openDirectScanner(config, fieldNames, readAheadBytes);
        return new TableScanCursor(
                cursor, cursor, entry -> Table.decodeDirectScannedRowOwned(compiled, entry));
    }

    private Object readResolve() throws ObjectStreamException {
        return new TableScanSplit(splitJson);
    }

    private void loadMetadata() {
        JsonObject split = JsonParser.parseString(splitJson).getAsJsonObject();
        JsonObject metadata = split.getAsJsonObject("metadata");
        schema = TableSchema.fromJson(metadata.getAsJsonObject("schema").toString());
        totalBuckets = split.get("total_buckets").getAsInt();
        if (totalBuckets < 1 || totalBuckets > 65536) {
            throw new IllegalArgumentException("split total_buckets must be in range 1..=65536");
        }
    }

    private static native NativeObject openScannerNative(
            String configJson,
            String splitJson,
            String[] fieldNames,
            int readAheadBytes,
            boolean direct);
}
