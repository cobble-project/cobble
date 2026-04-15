package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only snapshot of the database schema. */
public final class Schema implements Serializable {
    private static final long serialVersionUID = 718293745123L;
    private static final Gson GSON = new GsonBuilder().create();

    @SerializedName("version")
    public long version;

    @SerializedName("column_family_ids")
    public Map<String, Integer> columnFamilyIds = new LinkedHashMap<>();

    @SerializedName("column_families")
    public List<ColumnFamily> columnFamilies = new ArrayList<>();

    public static final class ColumnFamily implements Serializable {
        private static final long serialVersionUID = 718293745124L;

        @SerializedName("id")
        public int id;

        @SerializedName("name")
        public String name;

        @SerializedName("num_columns")
        public int numColumns;

        @SerializedName("operator_ids")
        public List<String> operatorIds = new ArrayList<>();

        public String mergeOperatorId(int columnIdx) {
            if (columnIdx < 0 || columnIdx >= operatorIds.size()) {
                return null;
            }
            return operatorIds.get(columnIdx);
        }
    }

    /** Get the default column family view. */
    public ColumnFamily defaultColumnFamily() {
        return columnFamily("default");
    }

    /** Get the merge operator id for a column in the default column family. */
    public String mergeOperatorId(int columnIdx) {
        ColumnFamily family = defaultColumnFamily();
        if (family == null) {
            return null;
        }
        return family.mergeOperatorId(columnIdx);
    }

    /** Get one column family view by name. */
    public ColumnFamily columnFamily(String columnFamily) {
        if (columnFamily == null) {
            return null;
        }
        for (ColumnFamily family : columnFamilies) {
            if (columnFamily.equals(family.name)) {
                return family;
            }
        }
        return null;
    }

    /** Get the merge operator id for a column in one column family. */
    public String mergeOperatorId(String columnFamily, int columnIdx) {
        ColumnFamily family = columnFamily(columnFamily);
        if (family == null) {
            return null;
        }
        return family.mergeOperatorId(columnIdx);
    }

    static Schema fromJson(String json) {
        return GSON.fromJson(json, Schema.class);
    }
}
