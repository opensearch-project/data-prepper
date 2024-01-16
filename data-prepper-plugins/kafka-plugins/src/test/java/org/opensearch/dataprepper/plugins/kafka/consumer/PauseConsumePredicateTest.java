/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PauseConsumePredicateTest {
    @Test
    void noPause_returns_predicate_with_pauseConsuming_returning_false() {
        final PauseConsumePredicate pauseConsumePredicate = PauseConsumePredicate.noPause();

        assertThat(pauseConsumePredicate, notNullValue());

        assertThat(pauseConsumePredicate.pauseConsuming(), equalTo(false));
    }

    @Test
    void circuitBreakingPredicate_with_a_null_circuit_breaker_returns_predicate_with_pauseConsuming_returning_false() {
        final PauseConsumePredicate pauseConsumePredicate = PauseConsumePredicate.circuitBreakingPredicate(null);

        assertThat(pauseConsumePredicate, notNullValue());

        assertThat(pauseConsumePredicate.pauseConsuming(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void circuitBreakingPredicate_with_a_circuit_breaker_returns_predicate_with_pauseConsuming_returning_value_of_isOpen(final boolean isOpen) {
        final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);

        final PauseConsumePredicate pauseConsumePredicate = PauseConsumePredicate.circuitBreakingPredicate(circuitBreaker);

        verifyNoInteractions(circuitBreaker);

        assertThat(pauseConsumePredicate, notNullValue());

        when(circuitBreaker.isOpen()).thenReturn(isOpen);

        assertThat(pauseConsumePredicate.pauseConsuming(), equalTo(isOpen));
    }
}