package io.cobble.table;

import java.util.Objects;

/** Internal versioned semantic schema plus physical layout. */
final class TableMetadata {
    static final String FORMAT = "cobble-table";
    static final int VERSION = 1;
    private final String format;
    private final int version;
    private final TableSchema schema;
    private final RecordLayout layout;

    TableMetadata(String format, int version, TableSchema schema, RecordLayout layout) {
        this.format = Objects.requireNonNull(format, "format");
        this.version = version;
        this.schema = Objects.requireNonNull(schema, "schema");
        this.layout = Objects.requireNonNull(layout, "layout");
        if (!FORMAT.equals(format) || version != VERSION)
            throw new IllegalArgumentException("unsupported table metadata format or version");
    }

    String format() {
        return format;
    }

    int version() {
        return version;
    }

    TableSchema schema() {
        return schema;
    }

    RecordLayout layout() {
        return layout;
    }

    String toJson() {
        return TableJson.toJson(this);
    }

    static TableMetadata fromJson(String json) {
        return TableJson.metadataFromJson(json);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TableMetadata)) return false;
        TableMetadata that = (TableMetadata) other;
        return version == that.version
                && format.equals(that.format)
                && schema.equals(that.schema)
                && layout.equals(that.layout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, version, schema, layout);
    }
}
