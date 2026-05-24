package io.cobble;

/** Process-level custom filesystem registry used as fallback from native filesystem resolution. */
public interface CustomFileSystemRegistry {
    /**
     * Tries to resolve a filesystem for the provided request.
     *
     * <p>Return {@code null} to decline handling and let native fallback continue.
     */
    CustomFileSystem tryResolve(ProcessFileSystemRequest request);
}
