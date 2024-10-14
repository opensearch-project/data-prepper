/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Data Prepper configuration for circuit breakers.
 *
 * @since 2.1
 */
public class CircuitBreakerConfig {
    @JsonProperty("heap")
    private HeapCircuitBreakerConfig heapConfig;

    /**
     * Gets the configuration for the heap.
     *
     * @return The heap circuit breaker configuration
     * @since 2.1
     */
    public HeapCircuitBreakerConfig getHeapConfig() {
        return heapConfig;
    }
}
