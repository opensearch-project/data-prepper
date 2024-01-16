/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.DelegatingBuffer;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Decorator for {@link Buffer} which checks a {@link CircuitBreaker}
 * before writing records.
 *
 * @param <T> The type of record.
 * @since 2.1
 */
class CircuitBreakingBuffer<T extends Record<?>> extends DelegatingBuffer<T> implements Buffer<T> {
    private final CircuitBreaker circuitBreaker;

    /**
     * Constructor
     *
     * @param buffer The inner buffer which is being decorated
     * @param circuitBreaker The circuit breaker to check
     */
    public CircuitBreakingBuffer(final Buffer<T> buffer, final CircuitBreaker circuitBreaker) {
        super(buffer);
        this.circuitBreaker = requireNonNull(circuitBreaker);
    }

    @Override
    public void write(final T record, final int timeoutInMillis) throws TimeoutException {
        checkBreaker();

        super.write(record, timeoutInMillis);
    }

    @Override
    public void writeAll(final Collection<T> records, final int timeoutInMillis) throws Exception {
        checkBreaker();

        super.writeAll(records, timeoutInMillis);
    }

    @Override
    public void writeBytes(final byte[] bytes, final String key, final int timeoutInMillis) throws Exception {
        checkBreaker();

        super.writeBytes(bytes, key, timeoutInMillis);
    }

    private void checkBreaker() throws TimeoutException {
        if(circuitBreaker.isOpen())
            throw new TimeoutException("Circuit breaker is open. Unable to write to buffer.");
    }
}
