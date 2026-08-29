package io.cobble;

/** Java-side filesystem contract bridged into Cobble core through JNI. */
public interface CustomFileSystem extends AutoCloseable {
    /** Creates a directory recursively relative to this filesystem root. */
    void createDir(String path);

    /** Returns whether the provided path exists. */
    boolean exists(String path);

    /** Returns the regular-file size in bytes, or {@code null} for a non-file path. */
    Long fileSize(String path);

    /**
     * Returns whether this filesystem can fast-copy {@code sourcePath} to the supplied filesystem
     * and path.
     */
    default boolean canFastCopyTo(
            String sourcePath, CustomFileSystem destinationFileSystem, String destinationPath) {
        return false;
    }

    /**
     * Fast-copies {@code sourcePath} to the supplied filesystem and path. Implementations must
     * leave the destination absent when throwing an exception.
     */
    default void fastCopyTo(
            String sourcePath, CustomFileSystem destinationFileSystem, String destinationPath) {
        throw new UnsupportedOperationException("Fast copy is not supported");
    }

    /** Deletes a file or directory path recursively. */
    void delete(String path);

    /** Schedules asynchronous deletion for the provided path. */
    void deleteAsync(String path);

    /** Renames or moves a path from {@code from} to {@code to}. */
    void rename(String from, String to);

    /** Lists child names under the provided path. */
    String[] list(String path);

    /** Opens a random-access reader for the provided path. */
    CustomRandomAccessFile openRead(String path);

    /** Opens a sequential writer for the provided path. */
    CustomSequentialWriteFile openWrite(String path);

    /** Returns unix-seconds last-modified timestamp, or {@code null} if unavailable. */
    Long lastModified(String path);

    /** Closes filesystem-level resources if any. */
    @Override
    void close();
}
