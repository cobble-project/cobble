package io.cobble;

/** Immutable native gauge value. */
public final class GaugeMetricValue implements MetricValue {
    private final double value;

    public GaugeMetricValue(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("gauge value must be finite");
        }
        this.value = value;
    }

    @Override
    public MetricType type() {
        return MetricType.GAUGE;
    }

    public double value() {
        return value;
    }
}
