package io.cobble.structured;

/** Controls which elements are retained when a list exceeds its maximum capacity. */
public enum ListRetainMode {
    /** Keep the oldest (first-inserted) elements. */
    FIRST("first"),
    /** Keep the newest (last-inserted) elements. */
    LAST("last");

    private final String id;

    ListRetainMode(String id) {
        this.id = id;
    }

    /** Returns the serialization identifier used in schema JSON. */
    public String getId() {
        return id;
    }

    /** Resolves a {@link ListRetainMode} from its serialization identifier. */
    public static ListRetainMode fromId(String id) {
        for (ListRetainMode mode : values()) {
            if (mode.id.equals(id)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown ListRetainMode: " + id);
    }
}
