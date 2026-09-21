package io.cobble;

import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.Serializable;

/**
 * Column-family scoped options applied by schema updates.
 *
 * <p>Behavior note: when {@link #valueHasTtl} is {@code false}, write-time TTL input (for example
 * from {@link WriteOptions#withTtl(int)}) is ignored for that column family.
 */
public final class ColumnFamilyOptions implements Serializable {
    private static final long serialVersionUID = 718293745125L;

    @SerializedName("value_has_ttl")
    public boolean valueHasTtl = true;

    /** Opaque, snapshot-captured JSON describing the logical format stored in this family. */
    @JsonAdapter(RawJsonAdapter.class)
    @SerializedName("metadata")
    public String metadata;

    public static ColumnFamilyOptions defaults() {
        return new ColumnFamilyOptions();
    }

    public ColumnFamilyOptions valueHasTtl(boolean valueHasTtl) {
        this.valueHasTtl = valueHasTtl;
        return this;
    }

    /** Sets snapshot-captured JSON metadata without changing the physical row format. */
    public ColumnFamilyOptions metadata(String metadata) {
        this.metadata = metadata;
        return this;
    }

    /** Reads and writes metadata as an embedded JSON value rather than a quoted JSON string. */
    private static final class RawJsonAdapter extends TypeAdapter<String> {
        @Override
        public void write(JsonWriter out, String value) throws java.io.IOException {
            if (value == null) {
                out.nullValue();
                return;
            }
            out.jsonValue(value);
        }

        @Override
        public String read(JsonReader in) throws java.io.IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            return JsonParser.parseReader(in).toString();
        }
    }
}
