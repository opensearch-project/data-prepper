package com.amazon.situp.model.buffer;

import com.amazon.situp.model.record.Record;

import java.util.Collection;

/**
 * SITUP buffer interface. Buffer queues the records between TI components and acts as a layer
 * between source and processor/sink. Buffer can be in-memory, disk based or other a standalone implementation.
 * <p>
 * TODO: Rename this such that it does not confuse java.nio.Buffer
 */
public interface Buffer<T extends Record<?>> {

    /**
     * writes the record to the buffer
     *
     * @param record The Record which needed to be written
     */
    void write(T record);

    /**
     * @return The earliest record in the buffer which is still not read.
     */
    T read();

    /**
     * @return Collection of records from the buffer
     */
    Collection<T> readBatch();

}
