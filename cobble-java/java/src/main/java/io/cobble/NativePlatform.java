package io.cobble;

/**
 * Detects the current OS/architecture and computes the resource paths and file names used to locate
 * bundled native artifacts (the JNI library and the {@code cobble-cli} executable) inside the JAR.
 *
 * <p>This is a package-private helper shared by {@link NativeLoader} and {@link CobbleCli} so that
 * platform detection is implemented in exactly one place.
 */
final class NativePlatform {
    private NativePlatform() {}

    /** Normalized OS identifier: {@code macos}, {@code linux}, or {@code windows}. */
    static String detectOs() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (osName.contains("mac") || osName.contains("darwin")) {
            return "macos";
        }
        if (osName.contains("win")) {
            return "windows";
        }
        if (osName.contains("nix") || osName.contains("nux") || osName.contains("linux")) {
            return "linux";
        }
        throw new IllegalStateException("Unsupported OS: " + osName);
    }

    /** Normalized architecture identifier: {@code x86_64} or {@code aarch64}. */
    static String detectArch() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        if (arch.equals("x86_64") || arch.equals("amd64")) {
            return "x86_64";
        }
        if (arch.equals("aarch64") || arch.equals("arm64")) {
            return "aarch64";
        }
        throw new IllegalStateException("Unsupported architecture: " + arch);
    }

    /** Resource directory shared by every native artifact, e.g. {@code native/macos-aarch64}. */
    static String resourceBase(NativeProfile profile) {
        return "native/" + detectOs() + "-" + detectArch() + "/" + profile.resourceSegment();
    }

    /** File name of the bundled JNI library for the current OS. */
    static String nativeLibraryFileName() {
        switch (detectOs()) {
            case "macos":
                return "libcobblejni.dylib";
            case "linux":
                return "libcobblejni.so";
            case "windows":
                return "cobblejni.dll";
            default:
                throw new IllegalStateException("Unsupported OS: " + detectOs());
        }
    }

    /** File name of the bundled {@code cobble-cli} executable for the current OS. */
    static String cliFileName() {
        return "cobble-cli" + (detectOs().equals("windows") ? ".exe" : "");
    }
}
