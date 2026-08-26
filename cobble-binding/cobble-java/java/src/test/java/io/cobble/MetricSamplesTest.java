package io.cobble;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Java-side typed metrics transport coverage. */
class MetricSamplesTest {

    @Test
    void parsesTypedImmutableMetricSamples() {
        List<MetricSample> samples =
                MetricSamples.fromJson(
                        "[{\"name\":\"count\",\"labels\":{\"kind\":\"index\"},\"type\":\"counter\",\"value\":7},"
                                + "{\"name\":\"files\",\"labels\":{},\"type\":\"gauge\",\"value\":2.5},"
                                + "{\"name\":\"ratio\",\"labels\":{\"compression\":\"lz4\"},\"type\":\"histogram\",\"value\":{\"count\":3,\"sum\":6.0,\"min\":1.0,\"max\":3.0}}]");

        assertEquals(3, samples.size());
        assertEquals(MetricType.COUNTER, samples.get(0).type());
        assertEquals(7L, ((CounterMetricValue) samples.get(0).value()).value());
        assertEquals(2.5d, ((GaugeMetricValue) samples.get(1).value()).value());
        HistogramMetricValue histogram = (HistogramMetricValue) samples.get(2).value();
        assertEquals(3L, histogram.count());
        assertEquals(6.0d, histogram.sum());
        assertThrows(
                UnsupportedOperationException.class,
                () -> samples.get(0).labels().put("unexpected", "value"));
        assertThrows(UnsupportedOperationException.class, () -> samples.add(samples.get(0)));
        assertTrue(samples.get(2).labels().containsKey("compression"));
    }

    @Test
    void rejectsInvalidMetricModels() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new MetricSample(
                                "",
                                Collections.<String, String>emptyMap(),
                                new CounterMetricValue(1L)));
        assertThrows(IllegalArgumentException.class, () -> new CounterMetricValue(-1L));
        assertThrows(IllegalArgumentException.class, () -> new GaugeMetricValue(Double.NaN));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GaugeMetricValue(Double.POSITIVE_INFINITY));
        Map<String, String> nullKey = new LinkedHashMap<String, String>();
        nullKey.put(null, "value");
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetricSample("count", nullKey, new CounterMetricValue(1L)));
        Map<String, String> nullValue = new LinkedHashMap<String, String>();
        nullValue.put("kind", null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetricSample("count", nullValue, new CounterMetricValue(1L)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new HistogramMetricValue(-1L, 0.0d, 0.0d, 0.0d));
        assertThrows(
                IllegalArgumentException.class,
                () -> new HistogramMetricValue(1L, Double.NaN, 0.0d, 0.0d));
        assertThrows(
                IllegalArgumentException.class,
                () -> new HistogramMetricValue(1L, 1.0d, 2.0d, 1.0d));
        assertDoesNotThrow(() -> new HistogramMetricValue(0L, 0.0d, 1.0d, 0.0d));
    }
}
