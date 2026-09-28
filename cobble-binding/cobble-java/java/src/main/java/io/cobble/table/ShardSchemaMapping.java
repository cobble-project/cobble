package io.cobble.table;

import java.io.Serializable;
import java.util.Objects;

/** One catalog-to-physical schema mapping reported after a connected shard materialization. */
public final class ShardSchemaMapping implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long tableId;
    private final String dbId;
    private final long catalogSchemaId;
    private final long coreSchemaId;

    ShardSchemaMapping(long tableId, String dbId, long catalogSchemaId, long coreSchemaId) {
        this.tableId = tableId;
        this.dbId = Objects.requireNonNull(dbId, "dbId");
        this.catalogSchemaId = catalogSchemaId;
        this.coreSchemaId = coreSchemaId;
    }

    public long tableId() {
        return tableId;
    }

    public String dbId() {
        return dbId;
    }

    public long catalogSchemaId() {
        return catalogSchemaId;
    }

    public long coreSchemaId() {
        return coreSchemaId;
    }
}
