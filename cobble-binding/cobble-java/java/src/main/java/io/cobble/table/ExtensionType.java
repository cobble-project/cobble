package io.cobble.table;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.util.Objects;

public final class ExtensionType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private final String typeId;
    private final String parametersJson;
    private final LogicalType physicalType;
    private transient volatile JsonElement parameters;

    ExtensionType(
            String typeId, String parametersJson, LogicalType physicalType, boolean nullable) {
        super(Kind.EXTENSION, nullable);
        this.typeId = Objects.requireNonNull(typeId, "typeId");
        this.parameters = parse(parametersJson);
        this.parametersJson = this.parameters.toString();
        this.physicalType = Objects.requireNonNull(physicalType, "physicalType");
        validate();
    }

    public String typeId() {
        return typeId;
    }

    public String parametersJson() {
        return parametersJson;
    }

    public LogicalType physicalType() {
        return physicalType;
    }

    @Override
    ExtensionType withNullability(boolean nullable) {
        return new ExtensionType(typeId, parametersJson, physicalType, nullable);
    }

    @Override
    public ExtensionType nullable() {
        return withNullability(true);
    }

    @Override
    public ExtensionType notNull() {
        return withNullability(false);
    }

    @Override
    void validate() {
        if (typeId.trim().isEmpty())
            throw new IllegalArgumentException("extension type id must not be empty");
        physicalType.validate();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ExtensionType
                && baseEquals((LogicalType) o)
                && typeId.equals(((ExtensionType) o).typeId)
                && parameters().equals(((ExtensionType) o).parameters())
                && physicalType.equals(((ExtensionType) o).physicalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), typeId, parameters(), physicalType);
    }

    private JsonElement parameters() {
        JsonElement parsed = parameters;
        if (parsed == null) {
            parsed = parse(parametersJson);
            parameters = parsed;
        }
        return parsed;
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            return new ExtensionType(typeId, parametersJson, physicalType, isNullable());
        } catch (RuntimeException e) {
            InvalidObjectException invalid = new InvalidObjectException("invalid extension type");
            invalid.initCause(e);
            throw invalid;
        }
    }

    private static JsonElement parse(String text) {
        try {
            return JsonParser.parseString(text == null ? "{}" : text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("extension parameters must be JSON", e);
        }
    }
}
