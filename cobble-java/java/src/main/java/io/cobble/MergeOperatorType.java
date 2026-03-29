package io.cobble;

/**
 * Describes a merge operator for use with {@link SchemaBuilder#setColumnOperator} and {@link
 * SchemaBuilder#addColumn}.
 *
 * <p>Use the predefined constants for built-in operators, or call {@link #list()} / {@link
 * #list(Integer, ListRetainMode, boolean)} for the list merge operator.
 */
public final class MergeOperatorType {

    /** Default merge operator — concatenates byte slices. */
    public static final MergeOperatorType BYTES =
            new MergeOperatorType("cobble::merge_operator::BytesMergeOperator", null);

    /** Merge operator that sums little-endian u32 values. */
    public static final MergeOperatorType U32_COUNTER =
            new MergeOperatorType("cobble::merge_operator::U32CounterMergeOperator", null);

    /** Merge operator that sums little-endian u64 values. */
    public static final MergeOperatorType U64_COUNTER =
            new MergeOperatorType("cobble::merge_operator::U64CounterMergeOperator", null);

    /** Retain mode for the list merge operator. */
    public enum ListRetainMode {
        FIRST("first"),
        LAST("last");

        final String id;

        ListRetainMode(String id) {
            this.id = id;
        }
    }

    /** Create a list merge operator with default config (no max, retain last, no element TTL). */
    public static MergeOperatorType list() {
        return list(null, ListRetainMode.LAST, false);
    }

    /**
     * Create a list merge operator with explicit config.
     *
     * @param maxElements maximum number of elements, or null for unlimited
     * @param retainMode which elements to retain when capped
     * @param preserveElementTtl whether to preserve per-element TTL
     */
    public static MergeOperatorType list(
            Integer maxElements, ListRetainMode retainMode, boolean preserveElementTtl) {
        StringBuilder sb = new StringBuilder("{");
        if (maxElements != null) {
            sb.append("\"max_elements\":").append(maxElements).append(",");
        } else {
            sb.append("\"max_elements\":null,");
        }
        sb.append("\"retain_mode\":\"").append(retainMode.id).append("\",");
        sb.append("\"preserve_element_ttl\":").append(preserveElementTtl);
        sb.append("}");
        return new MergeOperatorType("cobble.list.v1", sb.toString());
    }

    private final String operatorId;
    private final String metadataJson;

    private MergeOperatorType(String operatorId, String metadataJson) {
        this.operatorId = operatorId;
        this.metadataJson = metadataJson;
    }

    /** Return the Rust-side operator identifier string. */
    public String operatorId() {
        return operatorId;
    }

    /** Return the optional JSON metadata, or null. */
    String metadataJson() {
        return metadataJson;
    }
}
