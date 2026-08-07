/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.trace;

/**
 * Represents the attributes associated with an entire trace.
 * @since 1.2
 */
public interface TraceGroupFields {

    /**
     * Gets the end time of the trace in ISO 8601
     * @return the end time
     * @since 1.2
     */
    String getEndTime();

    /**
     * Gets the duration of the entire trace in nanoseconds
     * @return the duration
     * @since 1.2
     */
    Long getDurationInNanos();

    /**
     * Gets the status code for the entire trace
     * @return the status code
     * @since 1.2
     */
    Integer getStatusCode();
}
