package io.cobble;

/** Immutable native counter value. */
public final class CounterMetricValue implements MetricValue {
    private final long value;

    public CounterMetricValue(long value) {
        if (value < 0L) {
            throw new IllegalArgumentException("counter value must not be negative");
        }
        this.value = value;
    }

    @Override
    public MetricType type() {
        return MetricType.COUNTER;
    }

    public long value() {
        return value;
    }
}
