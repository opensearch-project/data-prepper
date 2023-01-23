/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

/**
 * Interface to signal that this {@link CircuitBreaker} to prevent
 * access beyond the {@link CircuitBreakerManager}.
 */
interface InnerCircuitBreaker extends CircuitBreaker {
}
