package io.cobble.table;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A resource-owning logical read session provided by a {@link TableReadProvider}.
 *
 * <p>Scan sessions are commonly fixed to one snapshot or checkpoint. Lookup sessions may expose an
 * explicit provider-specific refresh operation when their caller needs a newer snapshot.
 */
public interface TableReadSession<R, K> extends AutoCloseable {
    TableReadSchema schema();

    TableReadCapabilities capabilities();

    /** Full-key fields in the exact order required by {@link #lookup(Object)}, when supported. */
    default List<DataField> keyFields() {
        return Collections.emptyList();
    }

    /**
     * Opens a scan at the supplied position, or at its beginning.
     *
     * <p>A supplied position denotes progress immediately after one logical row, even when several
     * logical rows share one physical entry.
     */
    TableReadCursor<R> scan(TableReadRange range, TableReadPosition position) throws Exception;

    /** Performs an exact lookup. Providers without this capability reject the call. */
    Collection<TableReadEntry<R>> lookup(K key) throws Exception;

    @Override
    void close();
}
