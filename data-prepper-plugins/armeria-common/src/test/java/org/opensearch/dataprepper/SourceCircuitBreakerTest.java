/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SourceCircuitBreakerTest {
    @Mock
    private CircuitBreaker circuitBreaker;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Test
    void forBuffer_returns_the_circuit_breaker_if_the_buffer_is_not_written_off_heap_only() {
        when(buffer.isWrittenOffHeapOnly()).thenReturn(false);

        assertThat(new SourceCircuitBreaker(circuitBreaker).forBuffer(buffer), sameInstance(circuitBreaker));
    }

    @Test
    void forBuffer_returns_a_circuit_breaker_that_is_never_open_if_the_buffer_is_written_off_heap_only() {
        when(buffer.isWrittenOffHeapOnly()).thenReturn(true);

        final CircuitBreaker actualCircuitBreaker = new SourceCircuitBreaker(circuitBreaker).forBuffer(buffer);

        assertThat(actualCircuitBreaker, not(sameInstance(circuitBreaker)));
        assertThat(actualCircuitBreaker.isOpen(), equalTo(false));
        verifyNoInteractions(circuitBreaker);
    }

    @Test
    void forBuffer_returns_a_circuit_breaker_that_is_never_open_if_no_circuit_breaker_is_configured() {
        final CircuitBreaker actualCircuitBreaker = new SourceCircuitBreaker(null).forBuffer(buffer);

        assertThat(actualCircuitBreaker, notNullValue());
        assertThat(actualCircuitBreaker.isOpen(), equalTo(false));
        verifyNoInteractions(buffer);
    }

    @Test
    void SourceCircuitBreaker_does_not_implement_CircuitBreaker() {
        assertThat(CircuitBreaker.class.isAssignableFrom(SourceCircuitBreaker.class), equalTo(false));
    }
}
