/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the circuit breaker of a source and hands it out only for the buffer the source writes to.
 * <p>
 * A buffer that is written off heap only, like the Kafka buffer, does not add to the heap, so the
 * circuit breaker does not apply to a source writing to it. The pipeline follows the same rule for the
 * buffer-level circuit breaker. Sources keep this class instead of the {@link CircuitBreaker} itself,
 * so they cannot use the circuit breaker without this check.
 * <p>
 * {@link #forBuffer(Buffer)} never returns {@code null}. If no circuit breaker applies, it returns one that is
 * never open, so callers use the result without checks.
 * <p>
 * This class intentionally does not implement {@link CircuitBreaker}.
 */
public final class SourceCircuitBreaker {
    private static final Logger LOG = LoggerFactory.getLogger(SourceCircuitBreaker.class);
    private static final CircuitBreaker INACTIVE_CIRCUIT_BREAKER = () -> false;

    private final CircuitBreaker circuitBreaker;

    /**
     * @param circuitBreaker the circuit breaker of the source, or {@code null} if none is configured
     */
    public SourceCircuitBreaker(final CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Returns the circuit breaker that applies to a source writing to the given buffer.
     *
     * @param buffer the buffer the source writes to
     * @return the circuit breaker, or a circuit breaker that is never open if none is configured or the
     *         buffer is written off heap only
     */
    public CircuitBreaker forBuffer(final Buffer<?> buffer) {
        if (circuitBreaker == null) {
            return INACTIVE_CIRCUIT_BREAKER;
        }
        if (buffer.isWrittenOffHeapOnly()) {
            LOG.info("The buffer {} is written off heap only, so the circuit breaker does not apply to the source.",
                    buffer.getClass().getSimpleName());
            return INACTIVE_CIRCUIT_BREAKER;
        }
        return circuitBreaker;
    }
}
