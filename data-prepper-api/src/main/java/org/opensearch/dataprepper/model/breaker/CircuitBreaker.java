/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.breaker;

/**
 * Represents a circuit breaker in Data Prepper.
 *
 * @since 2.6
 */
public interface CircuitBreaker {
    /**
     * Checks a circuit breaker. If open, then the circuit breaker has
     * been tripped.
     *
     * @return true if open; false if closed.
     * @since 2.6
     */
    boolean isOpen();
}
