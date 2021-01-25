package com.amazon.dataprepper.model.buffer;

import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.CheckpointState;

import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * Buffer queues the records between TI components and acts as a layer between source and prepper/sink. Buffer can
 * be in-memory, disk based or other a standalone implementation.
 * <p>
 */
public interface Buffer<T extends Record<?>> {
    /**
     * writes the record to the buffer
     *
     * @param record the Record to add
     * @param timeoutInMillis how long to wait before giving up
     */
    void write(T record, int timeoutInMillis) throws TimeoutException;

    /**
     * Retrieves and removes the batch of records from the head of the queue. The batch size is defined/determined by
     * the configuration attribute "batch_size" or the @param timeoutInMillis
     * @param timeoutInMillis how long to wait before giving up
     * @return The earliest batch of records in the buffer which are still not read.
     */
    Collection<T> read(int timeoutInMillis);

    /**
     * Check summary of records processed by data-prepper downstreams(preppers, sinks, pipelines).
     *
     * @param checkpointState the summary object of checkpoint variables
     */
    void checkpoint(CheckpointState checkpointState);
}
