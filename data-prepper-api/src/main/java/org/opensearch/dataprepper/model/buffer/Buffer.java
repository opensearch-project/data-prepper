/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
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
     * Atomically writes bytes into the buffer
     *
     * @param bytes the bytes to be written to the buffer
     * @param key   key to use when writing to the buffer
     * @param timeoutInMillis how long to wait before giving up
     * @throws TimeoutException Unable to write to the buffer within the timeout
     * @throws SizeOverflowException The number of records exceeds the total capacity of the buffer. This cannot be retried.
     * @throws RuntimeException Other exceptions
     */
    default void writeBytes(final byte[] bytes, final String key, int timeoutInMillis) throws Exception {
        throw new UnsupportedOperationException("This buffer type does not support bytes.");
    }

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

    /**
     * Checks if the buffer is empty
     *
     * @return true if the buffer is empty, false otherwise
     */
    boolean isEmpty();

    /**
     * Checks if the buffer supports raw bytes
     *
     * @return true if the buffer supports raw bytes, false otherwise
     */
    default boolean isByteBuffer() {
        return false;
    }

    /**
     * Returns buffer's drain timeout as duration
     *
     * @return buffers drain timeout
     */
    default Duration getDrainTimeout() {
        return Duration.ZERO;
    }

    /**
     * Indicates if writes to this buffer are also in some way written
     * onto the JVM heap. If writes do go on heap, this should <b>false</b>
     * which is the default.
     *
     * @return True if this buffer does not write to the JVM heap.
     */
    default boolean isWrittenOffHeapOnly() {
        return false;
    }

    /**
     * shuts down the buffer
     */
    default void shutdown() {
    }
}
