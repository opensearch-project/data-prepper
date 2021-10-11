package com.amazon.dataprepper.model.span;

/**
 * Represents the attributes associated with an entire trace.
 */
public interface TraceGroupFields {

    /**
     * Gets the end time of the trace in ISO 8601
     * @return the end time
     */
    String getEndTime();

    /**
     * Gets the duration of the entire trace in nanoseconds
     * @return
     */
    String getDurationInNanos();

    /**
     * Gets the status code for the entire trace
     * @return the status code
     */
    Integer getStatusCod();
}
