package io.cobble.structured;

import java.util.Map;
import java.util.TreeMap;

/**
 * Defines the column types for a structured database.
 *
 * <p>Each column index maps to a {@link ColumnType}. Columns not explicitly defined default to
 * {@link ColumnType#BYTES}.
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

    private final String json;

    Schema(String json) {
        this.json = json;
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

    /** Builder for constructing a {@link Schema}. */
    public static class Builder {
        private final TreeMap<Integer, String> columns = new TreeMap<Integer, String>();

        Builder() {}

        /** Adds a bytes-typed column at the given index. */
        public Builder addBytesColumn(int columnIndex) {
            columns.put(columnIndex, "{\"kind\":\"bytes\"}");
            return this;
        }

        /** Adds a list-typed column at the given index with the specified configuration. */
        public Builder addListColumn(int columnIndex, ListConfig config) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"kind\":\"list\"");
            if (config.getMaxElements() != null) {
                sb.append(",\"max_elements\":").append(config.getMaxElements());
            }
            sb.append(",\"retain_mode\":\"").append(config.getRetainMode().getId()).append("\"");
            sb.append(",\"preserve_element_ttl\":").append(config.isPreserveElementTtl());
            sb.append("}");
            columns.put(columnIndex, sb.toString());
            return this;
        }

        /** Builds the schema. */
        public Schema build() {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"columns\":{");
            boolean first = true;
            for (Map.Entry<Integer, String> entry : columns.entrySet()) {
                if (!first) {
                    sb.append(",");
                }
                sb.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
                first = false;
            }
            sb.append("}}");
            return new Schema(sb.toString());
        }
    }
}
