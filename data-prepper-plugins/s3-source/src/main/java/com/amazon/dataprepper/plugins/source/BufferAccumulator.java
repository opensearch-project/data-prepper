/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Accumulates {@link Record} objects before placing them into a Data Prepper
 * {@link Buffer}. This class is not thread-safe and should only be used by one
 * thread at a time.
 *
 * @param <T> Type of record to accumulate
 */
class BufferAccumulator<T extends Record<?>> {
    private final Buffer<T> buffer;
    private final int numberOfRecordsToAccumulate;
    private final int bufferTimeoutMillis;

    private final Collection<T> recordsAccumulated;

    private BufferAccumulator(final Buffer<T> buffer, final int numberOfRecordsToAccumulate, final Duration bufferTimeout) {
        this.buffer = Objects.requireNonNull(buffer, "buffer must be non-null.");
        this.numberOfRecordsToAccumulate = numberOfRecordsToAccumulate;
        Objects.requireNonNull(bufferTimeout, "bufferTimeout must be non-null.");
        this.bufferTimeoutMillis = (int) bufferTimeout.toMillis();

        if(numberOfRecordsToAccumulate < 1)
            throw new IllegalArgumentException("numberOfRecordsToAccumulate must be greater than zero.");

        recordsAccumulated = new ArrayList<>(numberOfRecordsToAccumulate);
    }

    static <T extends Record<?>> BufferAccumulator<T> create(final Buffer<T> buffer, final int recordsToAccumulate, final Duration bufferTimeout) {
        return new BufferAccumulator<T>(buffer, recordsToAccumulate, bufferTimeout);
    }

    void add(final T record) throws Exception {
        recordsAccumulated.add(record);
        if (recordsAccumulated.size() == numberOfRecordsToAccumulate) {
            flushAccumulatedToBuffer();
        }
    }

    void flush() throws Exception {
        flushAccumulatedToBuffer();
    }

    private void flushAccumulatedToBuffer() throws Exception {
        if (recordsAccumulated.size() > 0) {
            buffer.writeAll(recordsAccumulated, bufferTimeoutMillis);
            recordsAccumulated.clear();
        }
    }
}
