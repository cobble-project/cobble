package io.cobble.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A table name within a catalog namespace. */
public final class TableIdentifier {
    private final List<String> namespace;
    private final String name;

    public TableIdentifier(List<String> namespace, String name) {
        Objects.requireNonNull(namespace, "namespace");
        ArrayList<String> copied = new ArrayList<String>(namespace.size());
        for (String component : namespace)
            copied.add(Objects.requireNonNull(component, "namespace component"));
        this.namespace = Collections.unmodifiableList(copied);
        this.name = Objects.requireNonNull(name, "name");
    }

    public List<String> namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    String toJson() {
        JsonObject object = new JsonObject();
        JsonArray parts = new JsonArray();
        for (String component : namespace) parts.add(component);
        object.add("namespace", parts);
        object.addProperty("name", name);
        return object.toString();
    }

    static TableIdentifier fromJson(JsonObject object) {
        JsonArray parts = object.getAsJsonArray("namespace");
        if (parts == null || object.get("name") == null)
            throw new IllegalArgumentException("invalid catalog table identifier");
        ArrayList<String> namespace = new ArrayList<String>(parts.size());
        for (int index = 0; index < parts.size(); index++)
            namespace.add(parts.get(index).getAsString());
        return new TableIdentifier(namespace, object.get("name").getAsString());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TableIdentifier)) return false;
        TableIdentifier that = (TableIdentifier) other;
        return namespace.equals(that.namespace) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }
}
