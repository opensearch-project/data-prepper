package com.amazon.situp.model.metrics;

public class MetricNames {
    /**
     * Metric representing the ingress of records to a pipeline component.
     * Applicable to processors and sinks
     */
    public static final String RECORDS_IN = "recordsIn";

    /**
     * Metric representing the egress of records from a pipeline component.
     * Applicable to processors and sources.
     */
    public static final String RECORDS_OUT = "recordsOut";

    /**
     * Metric representing the time elapsed during execution of a pipeline component.
     * Applicable to processors and sinks.
     */
    public static final String TIME_ELAPSED = "timeElapsed";

    /**
     * Delimiter used to separate path components in metric names.
     */
    public static final String DELIMITER = ".";
}
