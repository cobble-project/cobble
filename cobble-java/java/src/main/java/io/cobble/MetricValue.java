package io.cobble;

/** Immutable typed value carried by one {@link MetricSample}. */
public interface MetricValue {
    MetricType type();
}
