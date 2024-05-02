/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Data Prepper sink interface. Sink may publish records to a disk, a file,
 * to OpenSearch, other pipelines, or other external systems.
 */
public interface Sink<T extends Record<?>> {

    /**
     * outputs collection of records which extend {@link Record}.
     *
     * @param records the records to write to the sink.
     */
    void output(Collection<T> records);

    Object outputSync(Collection<T> records, boolean isQuery);

    /**
     * Prepare sink for shutdown, by cleaning up resources and threads.
     */
    void shutdown();

    /**
     * Initialize Sink
     */
    void initialize();

    /**
     * Indicates if Sink is ready to do output
     * @return returns true if the sink is ready, false otherwise
     */
    boolean isReady();

    /**
     * updates latency metrics of sink
     *
     * @param events list of events used for updating the latency metrics
     */
    default void updateLatencyMetrics(final Collection<T> events) {
    }

}
