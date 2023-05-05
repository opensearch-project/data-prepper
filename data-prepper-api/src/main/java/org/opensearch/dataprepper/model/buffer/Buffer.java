/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Buffer queues the records between TI components and acts as a layer between source and processor/sink. Buffer can
 * be in-memory, disk based or other a standalone implementation.
 */
public interface Buffer<T extends Record<?>> {
    /**
     * writes the record to the buffer
     *
     * @param record the Record to add
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException thrown when timeout for writing into the Buffer
     */
    void write(T record, int timeoutInMillis) throws TimeoutException;

    /**
     * Atomically writes collection of records into the buffer
     *
     * @param records the collection of records to add
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException Unable to write to the buffer within the timeout
     * @throws SizeOverflowException The number of records exceeds the total capacity of the buffer. This cannot be retried.
     * @throws RuntimeException Other exceptions
     */
    void writeAll(Collection<T> records, int timeoutInMillis) throws Exception;

    /**
     * Retrieves and removes the batch of records from the head of the queue. The batch size is defined/determined by
     * the configuration attribute "batch_size" or the @param timeoutInMillis
     * @param timeoutInMillis how long to wait before giving up
     * @return The earliest batch of records in the buffer which are still not read and its corresponding checkpoint state.
     */
    Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis);

    /**
     * Check summary of records processed by data-prepper downstreams(processors, sinks, pipelines).
     *
     * @param checkpointState the summary object of checkpoint variables
     */
    void checkpoint(CheckpointState checkpointState);

    boolean isEmpty();
}
