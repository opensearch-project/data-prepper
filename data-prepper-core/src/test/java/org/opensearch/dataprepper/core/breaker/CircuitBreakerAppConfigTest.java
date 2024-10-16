/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.breaker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.breaker.CircuitBreakerAppConfig;
import org.opensearch.dataprepper.core.breaker.HeapCircuitBreaker;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.core.parser.model.CircuitBreakerConfig;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerAppConfigTest {
    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;

    private CircuitBreakerAppConfig createObjectUnderTest() {
        return new CircuitBreakerAppConfig();
    }

    @Test
    void heapCircuitBreaker_returns_null_if_CircuitBreakerConfig_is_null() {
        assertThat(createObjectUnderTest().heapCircuitBreaker(dataPrepperConfiguration),
                nullValue());
    }

    @Test
    void heapCircuitBreaker_returns_null_if_HeapCircuitBreakerConfig_is_null() {
        final CircuitBreakerConfig circuitBreakerConfig = mock(CircuitBreakerConfig.class);
        when(dataPrepperConfiguration.getCircuitBreakerConfig())
                .thenReturn(circuitBreakerConfig);

        assertThat(createObjectUnderTest().heapCircuitBreaker(dataPrepperConfiguration),
                nullValue());
    }

    @Test
    void heapCircuitBreaker_returns_HeapCircuitBreaker_if_HeapCircuitBreakerConfig_is_present() {
        final ByteCount byteCount = mock(ByteCount.class);
        when(byteCount.getBytes()).thenReturn(1L);
        final HeapCircuitBreakerConfig heapCircuitBreakerConfig = mock(HeapCircuitBreakerConfig.class);
        when(heapCircuitBreakerConfig.getUsage()).thenReturn(byteCount);
        when(heapCircuitBreakerConfig.getCheckInterval()).thenReturn(Duration.ofSeconds(1));
        final CircuitBreakerConfig circuitBreakerConfig = mock(CircuitBreakerConfig.class);
        when(circuitBreakerConfig.getHeapConfig()).thenReturn(heapCircuitBreakerConfig);
        when(dataPrepperConfiguration.getCircuitBreakerConfig())
                .thenReturn(circuitBreakerConfig);

        assertThat(createObjectUnderTest().heapCircuitBreaker(dataPrepperConfiguration),
                instanceOf(HeapCircuitBreaker.class));
    }
}