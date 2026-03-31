package io.cobble.structured;

/**
 * Configuration for a list-type column.
 *
 * <p>Controls maximum capacity, element retention policy, and per-element TTL tracking.
 */
public class ListConfig {

    private final Integer maxElements;
    private final ListRetainMode retainMode;
    private final boolean preserveElementTtl;

    private ListConfig(Integer maxElements, ListRetainMode retainMode, boolean preserveElementTtl) {
        this.maxElements = maxElements;
        this.retainMode = retainMode;
        this.preserveElementTtl = preserveElementTtl;
    }

    /** Creates a default list config: unbounded capacity, retain-last, no element TTL. */
    public static ListConfig defaults() {
        return new ListConfig(null, ListRetainMode.LAST, false);
    }

    /** Creates a list config with specified max elements and retain mode. */
    public static ListConfig of(int maxElements, ListRetainMode retainMode) {
        return new ListConfig(maxElements, retainMode, false);
    }

    /**
     * Creates a list config with full customization.
     *
     * @param maxElements maximum number of elements, or null for unbounded
     * @param retainMode which elements to keep when capacity is exceeded
     * @param preserveElementTtl whether to track per-element TTL
     */
    public static ListConfig of(
            Integer maxElements, ListRetainMode retainMode, boolean preserveElementTtl) {
        return new ListConfig(maxElements, retainMode, preserveElementTtl);
    }

    /** Returns the maximum number of elements, or null if unbounded. */
    public Integer getMaxElements() {
        return maxElements;
    }

    /** Returns the element retention mode. */
    public ListRetainMode getRetainMode() {
        return retainMode;
    }

    /** Returns whether per-element TTL tracking is enabled. */
    public boolean isPreserveElementTtl() {
        return preserveElementTtl;
    }
}
