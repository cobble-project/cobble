package io.cobble.table;

import io.cobble.Config;
import io.cobble.NativeLoader;

import java.util.Objects;

/** Configures and terminally opens one catalog-scoped writable table shard. */
public final class TableWriterBuilder {
    private static final int OPEN = 0;
    private static final int RESUME = 1;
    private static final int OPEN_FROM_SNAPSHOT = 2;
    private static final int RESUME_FROM_SNAPSHOT = 3;

    private final CatalogTable catalogTable;
    private final TableWritePlan plan;
    private final Config runtime;
    private String dbId;
    private int[] rangeStarts = new int[0];
    private int[] rangeEnds = new int[0];

    private TableWriterBuilder(CatalogTable catalogTable, TableWritePlan plan, Config runtime) {
        this.catalogTable = catalogTable;
        this.plan = plan;
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    static TableWriterBuilder fromCatalog(CatalogTable table, Config runtime) {
        return new TableWriterBuilder(Objects.requireNonNull(table, "table"), null, runtime);
    }

    static TableWriterBuilder fromPlan(TableWritePlan plan, Config runtime) {
        return new TableWriterBuilder(null, Objects.requireNonNull(plan, "plan"), runtime);
    }

    /** Sets the durable shard database identity. */
    public TableWriterBuilder dbId(String value) {
        dbId = Objects.requireNonNull(value, "dbId");
        return this;
    }

    /** Sets the inclusive bucket ranges owned by this shard. */
    public TableWriterBuilder bucketRanges(int[] startsInclusive, int[] endsInclusive) {
        Objects.requireNonNull(startsInclusive, "startsInclusive");
        Objects.requireNonNull(endsInclusive, "endsInclusive");
        rangeStarts = startsInclusive.clone();
        rangeEnds = endsInclusive.clone();
        return this;
    }

    /** Opens a new writable shard. */
    public Table open() {
        return open(OPEN, -1L);
    }

    /** Resumes a writable shard. */
    public Table resume() {
        return open(RESUME, -1L);
    }

    /** Opens a new writable shard at a selected snapshot boundary. */
    public Table openFromSnapshot(long snapshotId) {
        validateSnapshotId(snapshotId);
        return open(OPEN_FROM_SNAPSHOT, snapshotId);
    }

    /** Resumes a writable shard at a selected snapshot boundary. */
    public Table resumeFromSnapshot(long snapshotId) {
        validateSnapshotId(snapshotId);
        return open(RESUME_FROM_SNAPSHOT, snapshotId);
    }

    private Table open(int mode, long snapshotId) {
        NativeLoader.load();
        if (catalogTable == null) {
            return TableWritePlan.writerOpenNative(
                    plan.nativePlanJson(),
                    runtime.toJson(),
                    dbId,
                    rangeStarts,
                    rangeEnds,
                    mode,
                    snapshotId);
        }
        synchronized (catalogTable) {
            return CatalogTable.writerOpenNative(
                    catalogTable.nativeHandleForBuilder(),
                    runtime.toJson(),
                    dbId,
                    rangeStarts,
                    rangeEnds,
                    mode,
                    snapshotId);
        }
    }

    private static void validateSnapshotId(long snapshotId) {
        if (snapshotId < 0L) throw new IllegalArgumentException("snapshotId must be >= 0");
    }
}
