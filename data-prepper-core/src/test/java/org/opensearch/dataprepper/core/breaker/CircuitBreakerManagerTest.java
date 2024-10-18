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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CircuitBreakerManagerTest {
    private List<InnerCircuitBreaker> innerCircuitBreakers;

    @BeforeEach
    void setUp() {
        innerCircuitBreakers = Collections.emptyList();
    }

    private CircuitBreakerManager createObjectUnderTest() {
        return new CircuitBreakerManager(innerCircuitBreakers);
    }

    @Test
    void constructor_throws_if_null_InnerCircuitBreakers() {
        innerCircuitBreakers = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getGlobalCircuitBreaker_returns_empty_if_list_is_empty() {
        final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

        assertThat(optionalCircuitBreaker, notNullValue());
        assertThat(optionalCircuitBreaker.isPresent(), equalTo(false));
    }

    @Nested
    class SingleCircuitBreaker {
        @Mock
        private InnerCircuitBreaker circuitBreaker;

        @BeforeEach
        void setUp() {
            innerCircuitBreakers = Collections.singletonList(circuitBreaker);
        }

        @ParameterizedTest
        @ValueSource(booleans = {false, true})
        void getGlobalCircuitBreaker_returns_CircuitBreaker_where_isOpen_is_equal_to_single_isOpen(final boolean innerIsOpen) {
            final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

            assertThat(optionalCircuitBreaker, notNullValue());
            assertThat(optionalCircuitBreaker.isPresent(), equalTo(true));
            assertThat(optionalCircuitBreaker.get(), notNullValue());

            when(circuitBreaker.isOpen()).thenReturn(innerIsOpen);
            final CircuitBreaker actualBreaker = optionalCircuitBreaker.get();
            assertThat(actualBreaker.isOpen(), equalTo(innerIsOpen));
        }
    }

    @Nested
    class MultipleCircuitBreakers {
        @BeforeEach
        void setUp() {
            innerCircuitBreakers = IntStream.range(0, 3)
                    .mapToObj(i -> mock(InnerCircuitBreaker.class))
                    .collect(Collectors.toList());
        }

        @Test
        void getGlobalCircuitBreaker_returns_CircuitBreaker_where_isOpen_is_equal_to_false_if_all_are_false() {
            for (InnerCircuitBreaker innerCircuitBreaker : innerCircuitBreakers) {
                when(innerCircuitBreaker.isOpen()).thenReturn(false);
            }

            final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

            assertThat(optionalCircuitBreaker, notNullValue());
            assertThat(optionalCircuitBreaker.isPresent(), equalTo(true));
            assertThat(optionalCircuitBreaker.get(), notNullValue());

            final CircuitBreaker actualBreaker = optionalCircuitBreaker.get();
            assertThat(actualBreaker.isOpen(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 1, 2})
        void getGlobalCircuitBreaker_returns_CircuitBreaker_where_isOpen_is_equal_to_true_if_any_are_true(final int openIndex) {
            for (int i = 0; i < 3; i++) {
                final InnerCircuitBreaker innerCircuitBreaker = innerCircuitBreakers.get(i);
                if(i == openIndex) {
                    when(innerCircuitBreaker.isOpen()).thenReturn(true);
                }
            }

            final Optional<CircuitBreaker> optionalCircuitBreaker = createObjectUnderTest().getGlobalCircuitBreaker();

            assertThat(optionalCircuitBreaker, notNullValue());
            assertThat(optionalCircuitBreaker.isPresent(), equalTo(true));
            assertThat(optionalCircuitBreaker.get(), notNullValue());

            final CircuitBreaker actualBreaker = optionalCircuitBreaker.get();
            assertThat(actualBreaker.isOpen(), equalTo(true));
        }
    }
}