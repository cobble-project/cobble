package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Read-only snapshot of the database schema. */
public final class Schema implements Serializable {
    private static final long serialVersionUID = 718293745123L;
    private static final Gson GSON = new GsonBuilder().create();

    @SerializedName("version")
    public long version;

    @SerializedName("num_columns")
    public int numColumns;

    @SerializedName("operator_ids")
    public List<String> operatorIds = new ArrayList<>();

    /** Get the merge operator id for a column. */
    public String mergeOperatorId(int columnIdx) {
        if (columnIdx < 0 || columnIdx >= operatorIds.size()) {
            return null;
        }
        return operatorIds.get(columnIdx);
    }

    static Schema fromJson(String json) {
        return GSON.fromJson(json, Schema.class);
    }
}
