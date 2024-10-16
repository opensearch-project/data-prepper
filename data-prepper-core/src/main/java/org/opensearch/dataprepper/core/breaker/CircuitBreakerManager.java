/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.breaker;

import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.util.List;
import java.util.Optional;

/**
 * Class for managing circuit breakers.
 *
 * @since 2.1
 */
public class CircuitBreakerManager {
    private final CircuitBreaker globalCircuitBreaker;

    CircuitBreakerManager(final List<InnerCircuitBreaker> circuitBreakers) {
        if(circuitBreakers.isEmpty()) {
            globalCircuitBreaker = null;
        } else {
            globalCircuitBreaker = new GlobalCircuitBreaker(circuitBreakers);
        }
    }

    /**
     * Returns a circuit breaker representing all circuit breakers. This is open
     * if and only if at least one circuit breaker is open.
     *
     * @return The global circuit breaker.
     */
    public Optional<CircuitBreaker> getGlobalCircuitBreaker() {
        return Optional.ofNullable(globalCircuitBreaker);
    }

    private static class GlobalCircuitBreaker implements CircuitBreaker {
        private final List<InnerCircuitBreaker> circuitBreakers;

        public GlobalCircuitBreaker(final List<InnerCircuitBreaker> circuitBreakers) {
            this.circuitBreakers = circuitBreakers;
        }

        @Override
        public boolean isOpen() {
            return circuitBreakers.stream().anyMatch(CircuitBreaker::isOpen);
        }
    }
}
