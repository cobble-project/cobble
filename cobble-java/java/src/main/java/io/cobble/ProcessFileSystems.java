package io.cobble;

/** Registers process-level custom filesystem fallback callbacks for Cobble native runtime. */
public final class ProcessFileSystems {
    private ProcessFileSystems() {}

    /**
     * Registers the process-level fallback registry.
     *
     * <p>This registry is consulted when native built-in filesystem resolution fails or when the
     * resolved filesystem cannot be accessed.
     */
    public static synchronized void registerCustomRegistry(CustomFileSystemRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry must not be null");
        }
        NativeLoader.load();
        registerCustomRegistryInternal(registry);
    }

    /** Clears the process-level fallback registry. */
    public static synchronized void clearCustomRegistry() {
        if (!NativeLoader.isLoaded()) {
            return;
        }
        clearCustomRegistryInternal();
    }

    private static native void registerCustomRegistryInternal(CustomFileSystemRegistry registry);

    private static native void clearCustomRegistryInternal();
}
