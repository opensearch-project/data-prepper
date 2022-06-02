/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Accumulates {@link Record} objects before placing them into a Data Prepper
 * {@link Buffer}. This class is not thread-safe and should only be used by one
 * thread at a time.
 *
 * @param <T> Type of record to accumulate
 */
class BufferAccumulator<T extends Record<?>> {
    private final Buffer<T> buffer;
    private final int recordsToAccumulate;
    private final int timeoutMillis;

    private final Collection<T> recordsAccumulated;

    private BufferAccumulator(final Buffer<T> buffer, final int recordsToAccumulate, final int timeoutMillis) {
        this.buffer = buffer;
        this.recordsToAccumulate = recordsToAccumulate;
        this.timeoutMillis = timeoutMillis;

        recordsAccumulated = new ArrayList<>(recordsToAccumulate);
    }

    static <T extends Record<?>> BufferAccumulator<T> create(final Buffer<T> buffer, final int recordsToAccumulate, final int timeoutMillis) {
        return new BufferAccumulator<T>(buffer, recordsToAccumulate, timeoutMillis);
    }

    void add(final T record) throws Exception {
        recordsAccumulated.add(record);
        if (recordsAccumulated.size() == recordsToAccumulate) {
            flushAccumulatedToBuffer();
        }
    }

    void flush() throws Exception {
        flushAccumulatedToBuffer();
    }

    private void flushAccumulatedToBuffer() throws Exception {
        if (recordsAccumulated.size() > 0) {
            buffer.writeAll(recordsAccumulated, timeoutMillis);
            recordsAccumulated.clear();
        }
    }
}
