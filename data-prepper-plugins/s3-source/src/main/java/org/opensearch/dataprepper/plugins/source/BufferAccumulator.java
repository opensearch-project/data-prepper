/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Accumulates {@link Record} objects before placing them into a Data Prepper
 * {@link Buffer}. This class is not thread-safe and should only be used by one
 * thread at a time.
 *
 * @param <T> Type of record to accumulate
 */
class BufferAccumulator<T extends Record<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(BufferAccumulator.class);

    private static final int MAX_FLUSH_RETRIES_ON_IO_EXCEPTION = Integer.MAX_VALUE;
    private static final Duration INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION = Duration.ofSeconds(5);

    private final Buffer<T> buffer;
    private final int numberOfRecordsToAccumulate;
    private final int bufferTimeoutMillis;
    private int totalWritten = 0;

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
        try {
            flushAccumulatedToBuffer();
        } catch (final TimeoutException timeoutException) {
            flushWithBackoff();
        }
    }

    private boolean flushWithBackoff() throws Exception{
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long nextDelay = INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION.toMillis();
        boolean flushedSuccessfully;

        for (int retryCount = 0; retryCount < MAX_FLUSH_RETRIES_ON_IO_EXCEPTION; retryCount++) {
            final ScheduledFuture<Boolean> flushBufferFuture = scheduledExecutorService.schedule(() -> {
                try {
                    flushAccumulatedToBuffer();
                    return true;
                } catch (final TimeoutException e) {
                    return false;
                }
            }, nextDelay, TimeUnit.MILLISECONDS);

            try {
                flushedSuccessfully = flushBufferFuture.get();
                if (flushedSuccessfully) {
                    LOG.info("Successfully flushed the buffer accumulator on retry attempt {}", retryCount + 1);
                    scheduledExecutorService.shutdownNow();
                    return true;
                }
            } catch (final ExecutionException e) {
                LOG.warn("Retrying of flushing the buffer accumulator hit an exception: {}", e.getMessage());
                scheduledExecutorService.shutdownNow();
                throw e;
            } catch (final InterruptedException e) {
                LOG.warn("Retrying of flushing the buffer accumulator was interrupted: {}", e.getMessage());
                scheduledExecutorService.shutdownNow();
                throw e;
            }
        }

        LOG.warn("Flushing the bufferAccumulator failed after {} attempts", MAX_FLUSH_RETRIES_ON_IO_EXCEPTION);
        scheduledExecutorService.shutdownNow();
        return false;
    }

    private void flushAccumulatedToBuffer() throws Exception {
        final int currentRecordCountAccumulated = recordsAccumulated.size();
        if (currentRecordCountAccumulated > 0) {
            buffer.writeAll(recordsAccumulated, bufferTimeoutMillis);
            recordsAccumulated.clear();
            totalWritten += currentRecordCountAccumulated;
        }
    }

    /**
     * Gets the total number of records written to the buffer.
     *
     * @return the total number of records written
     */
    public int getTotalWritten() {
        return totalWritten;
    }
}
