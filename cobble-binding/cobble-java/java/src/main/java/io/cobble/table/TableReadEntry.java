package io.cobble.table;

import java.util.Objects;

/** One decoded logical row and the durable position immediately after that row. */
public final class TableReadEntry<R> {
    private final TableReadPosition position;
    private final R value;
    private final long physicalBytes;
    private final boolean physicalEntry;

    public TableReadEntry(TableReadPosition position, R value) {
        this(position, value, 0L, false);
    }

    public TableReadEntry(
            TableReadPosition position, R value, long physicalBytes, boolean physicalEntry) {
        this.position = Objects.requireNonNull(position, "position");
        this.value = Objects.requireNonNull(value, "value");
        if (physicalBytes < 0L) throw new IllegalArgumentException("physicalBytes must be >= 0");
        this.physicalBytes = physicalBytes;
        this.physicalEntry = physicalEntry;
    }

    public TableReadPosition position() {
        return position;
    }

    public R value() {
        return value;
    }

    /** Number of native bytes read for this logical row's physical entry. */
    public long physicalBytes() {
        return physicalBytes;
    }

    /** Whether this row is the first logical row read from a physical entry. */
    public boolean countsPhysicalEntry() {
        return physicalEntry;
    }
}
