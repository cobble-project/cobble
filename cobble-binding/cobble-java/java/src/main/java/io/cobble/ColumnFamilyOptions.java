package io.cobble;

import com.google.gson.annotations.SerializedName;

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

    public static ColumnFamilyOptions defaults() {
        return new ColumnFamilyOptions();
    }

    public ColumnFamilyOptions valueHasTtl(boolean valueHasTtl) {
        this.valueHasTtl = valueHasTtl;
        return this;
    }
}
