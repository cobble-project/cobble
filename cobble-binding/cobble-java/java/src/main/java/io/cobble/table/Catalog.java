package io.cobble.table;

import java.util.List;

/** Semantic namespace, table, and schema-history operations for a catalog backend. */
public interface Catalog extends AutoCloseable {
    void createNamespace(List<String> namespace);

    List<List<String>> listNamespaces();

    void dropNamespace(List<String> namespace);

    CatalogTable createTable(TableIdentifier identifier, TableSchema schema);

    CatalogTable loadTable(TableIdentifier identifier);

    TableSchema loadTableSchema(TableIdentifier identifier, long catalogSchemaId);

    CatalogTable evolveSchema(TableIdentifier identifier, List<TableSchemaChange> changes);

    List<TableIdentifier> listTables(List<String> namespace);

    boolean tableExists(TableIdentifier identifier);

    CatalogTable renameTable(TableIdentifier identifier, String newName);

    void dropTable(TableIdentifier identifier);

    @Override
    void close();
}
