/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import org.opensearch.dataprepper.parser.model.CircuitBreakerConfig;

import java.util.Optional;

/**
 * Class for managing circuit breakers.
 *
 * @since 2.1
 */
public class CircuitBreakerService {
    private final HeapCircuitBreaker heapCircuitBreaker;

    CircuitBreakerService(final CircuitBreakerConfig circuitBreakerConfig) {
        if(circuitBreakerConfig != null && circuitBreakerConfig.getHeapConfig() != null) {
            heapCircuitBreaker = new HeapCircuitBreaker(circuitBreakerConfig.getHeapConfig());
        } else {
            heapCircuitBreaker = null;
        }
    }

    /**
     * Returns a circuit breaker representing all circuit breakers. This is open
     * if and only if at least one circuit breaker is open.
     *
     * @return The global circuit breaker.
     */
    public Optional<CircuitBreaker> getGlobalCircuitBreaker() {
        return Optional.ofNullable(heapCircuitBreaker);
    }
}
