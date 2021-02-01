package com.amazon.dataprepper.metrics;

public class MetricNames {
    private MetricNames() {}
    /**
     * Metric representing the ingress of records to a pipeline component.
     * Applicable to preppers and sinks
     */
    public static final String RECORDS_IN = "recordsIn";

    /**
     * Metric representing the egress of records from a pipeline component.
     * Applicable to preppers and sources.
     */
    public static final String RECORDS_OUT = "recordsOut";

    /**
     * Metric representing the time elapsed during execution of a pipeline component.
     * Applicable to preppers and sinks.
     */
    public static final String TIME_ELAPSED = "timeElapsed";

    /**
     * Metric representing the number of records written to a Buffer
     */
    public static final String RECORDS_WRITTEN = "recordsWritten";

    /**
     * Metric representing the number of records read from a buffer.
     */
    public static final String RECORDS_READ = "recordsRead";

    /**
     * Metric representing the number of records read from a buffer but unchecked.
     */
    public static final String RECORDS_INFLIGHT = "recordsInflight";

    /**
     * Metric representing the number of records read from a buffer and checked.
     */
    public static final String RECORDS_PROCESSED = "recordsProcessed";

    /**
     * Metric representing the time elapsed while writing to a Buffer
     */
    public static final String WRITE_TIME_ELAPSED = "writeTimeElapsed";

    /**
     * Metric representing the time elapsed while reading from a buffer
     */
    public static final String READ_TIME_ELAPSED = "readTimeElapsed";

    /**
     * Metric representing the time elapsed while checkpointing
     */
    public static final String CHECKPOINT_TIME_ELAPSED = "checkpointTimeElapsed";

    /**
     * Metric representing the count of write timeouts to a Buffer
     */
    public static final String WRITE_TIMEOUTS = "writeTimeouts";

    /**
     * Delimiter used to separate path components in metric names.
     */
    public static final String DELIMITER = ".";
}
