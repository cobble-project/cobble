package io.cobble;

/** Cobble utilities. */
public final class Utils {
    private Utils() {}

    public static native String versionString();

    public static native String buildCommitId();
}
