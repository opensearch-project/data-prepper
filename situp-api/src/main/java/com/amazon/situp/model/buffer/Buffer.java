package com.amazon.situp.model.buffer;

import com.amazon.situp.model.record.Record;

import java.util.Collection;

/**
 * Buffer queues the records between TI components and acts as a layer between source and processor/sink. Buffer can
 * be in-memory, disk based or other a standalone implementation.
 * <p>
 */
public interface Buffer<T extends Record<?>> {
    String ATTRIBUTE_BATCH_SIZE = "batch-size";
    int DEFAULT_BATCH_SIZE = 8;

    /**
     * writes the record to the buffer
     *
     * @param record the Record to add
     * @param timeoutInMillis how long to wait before giving up
     */
    void write(T record, int timeoutInMillis);

    /**
     * Retrieves and removes the batch of records from the head of the queue. The batch size is defined/determined by
     * the configuration attribute {@link #ATTRIBUTE_BATCH_SIZE} or the @param timeoutInMillis
     * @param timeoutInMillis how long to wait before giving up
     * @return The earliest batch of records in the buffer which are still not read.
     */
    Collection<T> read(int timeoutInMillis);

}
