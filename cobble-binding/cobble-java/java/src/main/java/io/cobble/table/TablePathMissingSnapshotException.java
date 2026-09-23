package io.cobble.table;

/** A native table path is valid but has no committed global snapshot yet. */
public final class TablePathMissingSnapshotException extends IllegalArgumentException {
    public TablePathMissingSnapshotException(String path) {
        super("Cobble table path has no committed global snapshot: " + path);
    }
}
