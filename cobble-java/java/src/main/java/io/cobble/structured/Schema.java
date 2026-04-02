package io.cobble.structured;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines the column types for a structured database.
 *
 * <p>Each column index maps to a {@link ColumnType}. Columns not explicitly defined default to
 * {@link ColumnType.Bytes}.
 *
 * <p>Use {@link #builder()} to create a schema:
 *
 * <pre>{@code
 * Schema schema = Schema.builder()
 *     .addBytesColumn(0)
 *     .addListColumn(1, ListConfig.of(100, ListRetainMode.LAST))
 *     .build();
 * }</pre>
 */
public class Schema {
    private static final Gson GSON =
            new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

    private final String json;
    private final Map<Integer, ColumnType> columns;

    Schema(String json) {
        this.json = json;
        this.columns = parseColumns(json);
    }

    /** Returns a new builder for constructing a schema. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the default schema where all columns are bytes-typed. */
    public static Schema defaults() {
        return new Schema("{\"columns\":{}}");
    }

    /** Returns the internal JSON representation used for JNI transport. */
    String toJson() {
        return json;
    }

    static Schema fromJson(String json) {
        return new Schema(json);
    }

    /** Returns the declared structured type of a column, or {@link ColumnType.Bytes} by default. */
    public ColumnType getColumnType(int columnIndex) {
        ColumnType type = columns.get(columnIndex);
        return type == null ? ColumnType.Bytes.INSTANCE : type;
    }

    /** Builder for constructing a {@link Schema}. */
    public static class Builder {
        private final HashMap<Integer, ColumnType> columns = new HashMap<Integer, ColumnType>();

        Builder() {}

        /** Adds a bytes-typed column at the given index. */
        public Builder addBytesColumn(int columnIndex) {
            columns.put(columnIndex, ColumnType.Bytes.INSTANCE);
            return this;
        }

        /** Adds a list-typed column at the given index with the specified configuration. */
        public Builder addListColumn(int columnIndex, ListConfig config) {
            if (config == null) {
                throw new IllegalArgumentException("config must not be null");
            }
            columns.put(columnIndex, new ColumnType.List(config));
            return this;
        }

        public Builder deleteColumn(int columnIndex) {
            columns.remove(columnIndex);
            return this;
        }

        /** Builds the schema. */
        public Schema build() {
            JsonObject root = new JsonObject();
            JsonObject out = new JsonObject();
            for (Map.Entry<Integer, ColumnType> entry : columns.entrySet()) {
                out.add(String.valueOf(entry.getKey()), toColumnJson(entry.getValue()));
            }
            root.add("columns", out);
            return new Schema(GSON.toJson(root));
        }
    }

    /** Column type hierarchy for structured schema definitions. */
    public abstract static class ColumnType {
        private ColumnType() {}

        /** Bytes column type (default). */
        public static final class Bytes extends ColumnType {
            public static final Bytes INSTANCE = new Bytes();

            private Bytes() {}
        }

        /** List column type with list behavior config. */
        public static final class List extends ColumnType {
            private final ListConfig config;

            private List(ListConfig config) {
                this.config = config;
            }

            public ListConfig getConfig() {
                return config;
            }
        }
    }

    private static Map<Integer, ColumnType> parseColumns(String json) {
        JsonObject root = GSON.fromJson(json, JsonObject.class);
        if (root == null || !root.has("columns") || !root.get("columns").isJsonObject()) {
            return Collections.<Integer, ColumnType>emptyMap();
        }
        JsonObject columnObj = root.getAsJsonObject("columns");
        Map<Integer, ColumnType> out = new HashMap<Integer, ColumnType>();
        for (Map.Entry<String, JsonElement> entry : columnObj.entrySet()) {
            int columnIndex;
            try {
                columnIndex = Integer.parseInt(entry.getKey());
            } catch (NumberFormatException ignored) {
                continue;
            }
            if (!entry.getValue().isJsonObject()) {
                continue;
            }
            JsonObject def = entry.getValue().getAsJsonObject();
            String kind = def.has("kind") ? def.get("kind").getAsString() : "bytes";
            if ("list".equals(kind)) {
                Integer maxElements =
                        def.has("max_elements") && !def.get("max_elements").isJsonNull()
                                ? def.get("max_elements").getAsInt()
                                : null;
                String retain =
                        def.has("retain_mode") ? def.get("retain_mode").getAsString() : "last";
                boolean preserveElementTtl =
                        def.has("preserve_element_ttl")
                                && def.get("preserve_element_ttl").getAsBoolean();
                ListRetainMode mode =
                        "first".equals(retain) ? ListRetainMode.FIRST : ListRetainMode.LAST;
                out.put(
                        columnIndex,
                        new ColumnType.List(ListConfig.of(maxElements, mode, preserveElementTtl)));
            } else {
                out.put(columnIndex, ColumnType.Bytes.INSTANCE);
            }
        }
        return Collections.unmodifiableMap(out);
    }

    private static JsonObject toColumnJson(ColumnType type) {
        JsonObject obj = new JsonObject();
        if (type instanceof ColumnType.List) {
            ColumnType.List listType = (ColumnType.List) type;
            ListConfig cfg = listType.getConfig();
            obj.addProperty("kind", "list");
            if (cfg.getMaxElements() == null) {
                obj.add("max_elements", JsonNull.INSTANCE);
            } else {
                obj.addProperty("max_elements", cfg.getMaxElements());
            }
            obj.addProperty("retain_mode", cfg.getRetainMode().getId());
            obj.addProperty("preserve_element_ttl", cfg.isPreserveElementTtl());
            return obj;
        }
        obj.addProperty("kind", "bytes");
        return obj;
    }
}
