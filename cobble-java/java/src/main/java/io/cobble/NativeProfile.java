package io.cobble;

/**
 * The profile that the native library is built with. This can be used to select different native
 * libraries for debugging or release purposes.
 */
public enum NativeProfile {
    DEBUG("debug"),
    RELEASE("release");

    private final String resourceSegment;

    NativeProfile(String resourceSegment) {
        this.resourceSegment = resourceSegment;
    }

    public String resourceSegment() {
        return resourceSegment;
    }

    public static NativeProfile fromString(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return RELEASE;
        }
        if ("debug".equalsIgnoreCase(raw)) {
            return DEBUG;
        }
        if ("release".equalsIgnoreCase(raw)) {
            return RELEASE;
        }
        throw new IllegalArgumentException("Unsupported native profile: " + raw);
    }
}
