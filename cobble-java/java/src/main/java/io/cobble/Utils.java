package io.cobble;

/** Cobble utilities. */
public final class Utils {
    private Utils() {}

    public static String versionString() {
        return nativeVersionString0();
    }

    public static String buildCommitId() {
        return nativeBuildCommitId0();
    }

    private static native String nativeVersionString0();

    private static native String nativeBuildCommitId0();
}
