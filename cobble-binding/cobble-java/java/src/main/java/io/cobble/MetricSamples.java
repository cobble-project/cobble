package io.cobble;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON transport parser used by native binding helpers. */
final class MetricSamples {
    private MetricSamples() {}

    static List<MetricSample> fromJson(String json) {
        JsonArray values = JsonParser.parseString(json).getAsJsonArray();
        List<MetricSample> samples = new ArrayList<MetricSample>(values.size());
        for (JsonElement element : values) {
            JsonObject sample = element.getAsJsonObject();
            samples.add(
                    new MetricSample(
                            sample.get("name").getAsString(),
                            labels(sample.getAsJsonObject("labels")),
                            value(sample)));
        }
        return Collections.unmodifiableList(samples);
    }

    private static Map<String, String> labels(JsonObject labels) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<String, JsonElement> entry : labels.entrySet()) {
            values.put(entry.getKey(), entry.getValue().getAsString());
        }
        return values;
    }

    private static MetricValue value(JsonObject sample) {
        String type = sample.get("type").getAsString();
        JsonElement value = sample.get("value");
        if ("counter".equals(type)) {
            return new CounterMetricValue(value.getAsLong());
        }
        if ("gauge".equals(type)) {
            return new GaugeMetricValue(value.getAsDouble());
        }
        if ("histogram".equals(type)) {
            JsonObject histogram = value.getAsJsonObject();
            return new HistogramMetricValue(
                    histogram.get("count").getAsLong(),
                    histogram.get("sum").getAsDouble(),
                    histogram.get("min").getAsDouble(),
                    histogram.get("max").getAsDouble());
        }
        throw new IllegalArgumentException("unknown Cobble metric type: " + type);
    }
}
