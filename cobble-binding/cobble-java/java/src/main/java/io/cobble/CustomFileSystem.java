package io.cobble;

/** Java-side filesystem contract bridged into Cobble core through JNI. */
public interface CustomFileSystem extends AutoCloseable {
    /** Creates a directory recursively relative to this filesystem root. */
    void createDir(String path);

    /** Returns whether the provided path exists. */
    boolean exists(String path);

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
