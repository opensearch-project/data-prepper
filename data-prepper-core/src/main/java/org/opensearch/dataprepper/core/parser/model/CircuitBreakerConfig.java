/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
