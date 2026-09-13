package io.cobble.table;

import io.cobble.Config;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** File-backed catalog for namespaces, table identities, and table schema history. */
public final class FileCatalog extends NativeObject {
    private static final Gson GSON = new Gson();

    private FileCatalog(long nativeHandle) {
        super(nativeHandle);
    }

    public static FileCatalog open(Config config, String storageId) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(storageId, "storageId");
        NativeLoader.load();
        long handle = openNative(config.toJson(), storageId);
        if (handle == 0L) throw new IllegalStateException("failed to open file catalog");
        return new FileCatalog(handle);
    }

    public synchronized void createNamespace(List<String> namespace) {
        createNamespaceNative(openHandle(), namespaceJson(namespace));
    }

    public synchronized List<List<String>> listNamespaces() {
        JsonArray values =
                JsonParser.parseString(listNamespacesNative(openHandle())).getAsJsonArray();
        ArrayList<List<String>> namespaces = new ArrayList<List<String>>(values.size());
        for (JsonElement value : values) namespaces.add(namespaceFromJson(value.getAsJsonArray()));
        return Collections.unmodifiableList(namespaces);
    }

    public synchronized void dropNamespace(List<String> namespace) {
        dropNamespaceNative(openHandle(), namespaceJson(namespace));
    }

    public synchronized CatalogTable createTable(TableIdentifier identifier, TableSchema schema) {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(schema, "schema");
        return CatalogTable.fromNativeHandle(
                createTableNative(openHandle(), identifier.toJson(), TableJson.toJson(schema)));
    }

    public synchronized CatalogTable loadTable(TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return CatalogTable.fromNativeHandle(loadTableNative(openHandle(), identifier.toJson()));
    }

    public synchronized TableSchema loadTableSchema(
            TableIdentifier identifier, long catalogSchemaId) {
        Objects.requireNonNull(identifier, "identifier");
        if (catalogSchemaId < 0L || catalogSchemaId > 0xffffffffL)
            throw new IllegalArgumentException("catalogSchemaId out of range");
        return TableSchema.fromJson(
                loadTableSchemaNative(openHandle(), identifier.toJson(), catalogSchemaId));
    }

    public synchronized CatalogTable evolveSchema(
            TableIdentifier identifier, List<TableSchemaChange> changes) {
        Objects.requireNonNull(identifier, "identifier");
        return CatalogTable.fromNativeHandle(
                evolveSchemaNative(
                        openHandle(), identifier.toJson(), TableSchemaChange.toJson(changes)));
    }

    public synchronized List<TableIdentifier> listTables(List<String> namespace) {
        JsonArray values =
                JsonParser.parseString(listTablesNative(openHandle(), namespaceJson(namespace)))
                        .getAsJsonArray();
        ArrayList<TableIdentifier> tables = new ArrayList<TableIdentifier>(values.size());
        for (JsonElement value : values)
            tables.add(TableIdentifier.fromJson(value.getAsJsonObject()));
        return Collections.unmodifiableList(tables);
    }

    public synchronized boolean tableExists(TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return tableExistsNative(openHandle(), identifier.toJson());
    }

    public synchronized CatalogTable renameTable(TableIdentifier identifier, String newName) {
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(newName, "newName");
        return CatalogTable.fromNativeHandle(
                renameTableNative(openHandle(), identifier.toJson(), newName));
    }

    public synchronized void dropTable(TableIdentifier identifier) {
        Objects.requireNonNull(identifier, "identifier");
        dropTableNative(openHandle(), identifier.toJson());
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private long openHandle() {
        if (isDisposed() || nativeHandle == 0L)
            throw new IllegalStateException("file catalog is closed");
        return nativeHandle;
    }

    private static String namespaceJson(List<String> namespace) {
        Objects.requireNonNull(namespace, "namespace");
        for (String component : namespace) Objects.requireNonNull(component, "namespace component");
        return GSON.toJson(namespace);
    }

    private static List<String> namespaceFromJson(JsonArray values) {
        ArrayList<String> namespace = new ArrayList<String>(values.size());
        for (JsonElement value : values) namespace.add(value.getAsString());
        return Collections.unmodifiableList(namespace);
    }

    private static native long openNative(String configJson, String storageId);

    private static native void createNamespaceNative(long nativeHandle, String namespaceJson);

    private static native String listNamespacesNative(long nativeHandle);

    private static native void dropNamespaceNative(long nativeHandle, String namespaceJson);

    private static native long createTableNative(
            long nativeHandle, String identifierJson, String schemaJson);

    private static native long loadTableNative(long nativeHandle, String identifierJson);

    private static native String loadTableSchemaNative(
            long nativeHandle, String identifierJson, long catalogSchemaId);

    private static native long evolveSchemaNative(
            long nativeHandle, String identifierJson, String changesJson);

    private static native String listTablesNative(long nativeHandle, String namespaceJson);

    private static native boolean tableExistsNative(long nativeHandle, String identifierJson);

    private static native long renameTableNative(
            long nativeHandle, String identifierJson, String newName);

    private static native void dropTableNative(long nativeHandle, String identifierJson);
}
