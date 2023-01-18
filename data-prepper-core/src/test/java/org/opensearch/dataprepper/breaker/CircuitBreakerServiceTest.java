/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.breaker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.parser.model.CircuitBreakerConfig;
import org.opensearch.dataprepper.parser.model.HeapCircuitBreakerConfig;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerServiceTest {
    @Mock
    private CircuitBreakerConfig circuitBreakerConfig;

    private CircuitBreakerService createObjectUnderTest() {
        return new CircuitBreakerService(circuitBreakerConfig);
    }

    @Test
    void getGlobalCircuitBreaker_returns_empty_if_config_is_null() {
        circuitBreakerConfig = null;

        final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

        assertThat(optionalCircuitBreaker, notNullValue());
        assertThat(optionalCircuitBreaker.isPresent(), equalTo(false));
    }

    @Test
    void getGlobalCircuitBreaker_returns_empty_if_heap_is_null() {
        when(circuitBreakerConfig.getHeapConfig()).thenReturn(null);

        final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

        assertThat(optionalCircuitBreaker, notNullValue());
        assertThat(optionalCircuitBreaker.isPresent(), equalTo(false));
    }

    @Test
    void getGlobalCircuitBreaker_returns_HeapCircuitBreaker_if_heap_is_present() {
        final HeapCircuitBreakerConfig heapConfig = mock(HeapCircuitBreakerConfig.class);
        final ByteCount byteCount = mock(ByteCount.class);
        when(byteCount.getBytes()).thenReturn(1L);
        when(heapConfig.getUsage()).thenReturn(byteCount);
        when(circuitBreakerConfig.getHeapConfig()).thenReturn(heapConfig);

        final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

        assertThat(optionalCircuitBreaker, notNullValue());
        assertThat(optionalCircuitBreaker.isPresent(), equalTo(true));
        assertThat(optionalCircuitBreaker.get(), instanceOf(HeapCircuitBreaker.class));
    }
}