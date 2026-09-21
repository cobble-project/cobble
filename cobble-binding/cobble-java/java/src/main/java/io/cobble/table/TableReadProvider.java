package io.cobble.table;

/** Opens resource-owning logical read sessions over a pluggable table-shaped read source. */
public interface TableReadProvider<R, K> extends AutoCloseable {
    TableReadCapabilities capabilities();

    TableReadSession<R, K> open() throws Exception;

    @Override
    void close();
}
