package io.cobble.table;

/** Declares the operations and ordering guarantees made by a table read provider. */
public final class TableReadCapabilities {
    private final boolean scan;
    private final boolean exactLookup;
    private final boolean restartableScan;

    public TableReadCapabilities(boolean scan, boolean exactLookup, boolean restartableScan) {
        this.scan = scan;
        this.exactLookup = exactLookup;
        this.restartableScan = restartableScan;
    }

    public boolean scan() {
        return scan;
    }

    public boolean exactLookup() {
        return exactLookup;
    }

    public boolean restartableScan() {
        return restartableScan;
    }
}
