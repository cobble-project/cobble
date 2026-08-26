package io.cobble;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable native Cobble metric sample. */
public final class MetricSample {
    private final String name;
    private final Map<String, String> labels;
    private final MetricValue value;

    public MetricSample(String name, Map<String, String> labels, MetricValue value) {
        if (name == null || name.isEmpty() || labels == null || value == null) {
            throw new IllegalArgumentException(
                    "name must not be empty; labels and value must not be null");
        }
        for (Map.Entry<String, String> label : labels.entrySet()) {
            if (label.getKey() == null || label.getValue() == null) {
                throw new IllegalArgumentException("metric label keys and values must not be null");
            }
        }
        this.name = name;
        this.labels = Collections.unmodifiableMap(new LinkedHashMap<String, String>(labels));
        this.value = value;
    }

    public String name() {
        return name;
    }

    public Map<String, String> labels() {
        return labels;
    }

    public MetricValue value() {
        return value;
    }

    public MetricType type() {
        return value.type();
    }
}
