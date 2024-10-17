/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.breaker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.CircuitBreakerConfig;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CircuitBreakerIT {
    private static final Duration SMALL_SLEEP_INTERVAL = Duration.ofMillis(50);
    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;
    @Mock
    private CircuitBreakerConfig circuitBreakerConfig;
    @BeforeEach
    void setUp() {
        when(dataPrepperConfiguration.getCircuitBreakerConfig()).thenReturn(circuitBreakerConfig);
    }

    private CircuitBreakerManager createObjectUnderTest() {
        final AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.registerBean("dataPrepperConfig", DataPrepperConfiguration.class, () -> dataPrepperConfiguration);
        applicationContext.scan(CircuitBreakerAppConfig.class.getPackage().getName());
        applicationContext.register(CircuitBreakerAppConfig.class);
        applicationContext.refresh();

        return applicationContext.getBean(CircuitBreakerManager.class);
    }

    @Test
    void globalCircuitBreaker_returns_empty_when_no_circuit_breakers_active() {
        final Optional<CircuitBreaker> globalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();
        assertThat(globalCircuitBreaker, notNullValue());
        assertThat(globalCircuitBreaker.isPresent(), equalTo(false));
    }

    @Nested
    class HeapCircuitBreaker {
        @Mock
        private HeapCircuitBreakerConfig heapCircuitBreakerConfig;

        @BeforeEach
        void setUp() {
            when(circuitBreakerConfig.getHeapConfig()).thenReturn(heapCircuitBreakerConfig);
            when(heapCircuitBreakerConfig.getCheckInterval()).thenReturn(SMALL_SLEEP_INTERVAL);
        }

        @ParameterizedTest
        @CsvSource({
                "1000000gb, false",
                "8b, true"
        })
        void globalCircuitBreaker_returns_expected_value_based_on_heap(final String byteCount, final boolean expectedIsOpen) throws InterruptedException {
            when(heapCircuitBreakerConfig.getUsage()).thenReturn(ByteCount.parse(byteCount));
            final Optional<CircuitBreaker> globalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

            assertThat(globalCircuitBreaker, notNullValue());
            assertThat(globalCircuitBreaker.isPresent(), equalTo(true));
            final CircuitBreaker circuitBreaker = globalCircuitBreaker.get();
            assertThat(circuitBreaker, notNullValue());

            Thread.sleep(SMALL_SLEEP_INTERVAL.toMillis());

            assertThat(circuitBreaker.isOpen(), equalTo(expectedIsOpen));
        }
    }
}
