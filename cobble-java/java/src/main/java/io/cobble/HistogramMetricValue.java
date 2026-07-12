package io.cobble;

/** Immutable native histogram summary. */
public final class HistogramMetricValue implements MetricValue {
    private final long count;
    private final double sum;
    private final double min;
    private final double max;

    public HistogramMetricValue(long count, double sum, double min, double max) {
        if (count < 0L) {
            throw new IllegalArgumentException("histogram count must not be negative");
        }
        if (!Double.isFinite(sum) || !Double.isFinite(min) || !Double.isFinite(max)) {
            throw new IllegalArgumentException("histogram sum, min, and max must be finite");
        }
        if (count > 0L && min > max) {
            throw new IllegalArgumentException(
                    "histogram min must not exceed max when count is positive");
        }
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    @Override
    public MetricType type() {
        return MetricType.HISTOGRAM;
    }

    public long count() {
        return count;
    }

    public double sum() {
        return sum;
    }

    public double min() {
        return min;
    }

    public double max() {
        return max;
    }
}
