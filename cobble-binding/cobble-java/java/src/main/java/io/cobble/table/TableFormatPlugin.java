package io.cobble.table;

import io.cobble.Config;

import java.util.Optional;

/** Service-loaded implementation for one exact table format and optional external path layout. */
public interface TableFormatPlugin {
    /** Exact {@code format} value stored in the selected column family's snapshot metadata. */
    String formatId();

    /** Interprets one fixed snapshot descriptor into session-private schema and codec state. */
    TableFormatBinding bind(TableReadSnapshot snapshot) throws Exception;

    /**
     * Recognizes and pins an external location for this format.
     *
     * <p>An empty result means only that this plugin does not own the path. Recognized but broken
     * paths must fail with their diagnostic rather than falling through to another plugin.
     */
    default Optional<TableReadSnapshot> resolvePath(Config config, TablePathRequest request)
            throws Exception {
        return Optional.empty();
    }
}
