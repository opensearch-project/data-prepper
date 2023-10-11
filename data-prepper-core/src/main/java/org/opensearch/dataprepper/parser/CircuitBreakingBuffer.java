/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.opensearch.dataprepper.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Decorator for {@link Buffer} which checks a {@link CircuitBreaker}
 * before writing records.
 *
 * @param <T> The type of record.
 * @since 2.1
 */
class CircuitBreakingBuffer<T extends Record<?>> implements Buffer<T> {
    private final Buffer<T> buffer;
    private final CircuitBreaker circuitBreaker;

    /**
     * Constructor
     *
     * @param buffer The inner buffer which is being decorated
     * @param circuitBreaker The circuit breaker to check
     */
    public CircuitBreakingBuffer(final Buffer<T> buffer, final CircuitBreaker circuitBreaker) {
        this.buffer = requireNonNull(buffer);
        this.circuitBreaker = requireNonNull(circuitBreaker);
    }

    @Override
    public void write(final T record, final int timeoutInMillis) throws TimeoutException {
        checkBreaker();

        buffer.write(record, timeoutInMillis);
    }

    @Override
    public void writeAll(final Collection<T> records, final int timeoutInMillis) throws Exception {
        checkBreaker();

        buffer.writeAll(records, timeoutInMillis);
    }

    private void checkBreaker() throws TimeoutException {
        if(circuitBreaker.isOpen())
            throw new TimeoutException("Circuit breaker is open. Unable to write to buffer.");
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(final int timeoutInMillis) {
        return buffer.read(timeoutInMillis);
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        buffer.checkpoint(checkpointState);
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public Duration getDrainTimeout() {
        return buffer.getDrainTimeout();
    }

    @Override
    public void shutdown() {
        buffer.shutdown();
    }
}
