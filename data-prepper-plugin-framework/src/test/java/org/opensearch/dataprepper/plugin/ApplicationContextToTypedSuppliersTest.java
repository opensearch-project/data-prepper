/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.event.EventFactory;

import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class ApplicationContextToTypedSuppliersTest {
    @Mock
    private EventFactory eventFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private CircuitBreaker circuitBreaker;

    private ApplicationContextToTypedSuppliers createObjectUnderTest() {
        return new ApplicationContextToTypedSuppliers(
                eventFactory,
                acknowledgementSetManager,
                circuitBreaker
        );
    }

    @Test
    void constructor_throws_with_null_EventFactory() {
        eventFactory = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_AcknowledgementSetManager() {
        acknowledgementSetManager = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getArgumentsSuppliers_returns_map_with_expected_classes() {
        final Map<Class<?>, Supplier<Object>> argumentsSuppliers = createObjectUnderTest().getArgumentsSuppliers();

        assertThat(argumentsSuppliers.size(), equalTo(3));

        assertThat(argumentsSuppliers, hasKey(EventFactory.class));
        assertThat(argumentsSuppliers.get(EventFactory.class), notNullValue());
        assertThat(argumentsSuppliers.get(EventFactory.class).get(), equalTo(eventFactory));

        assertThat(argumentsSuppliers, hasKey(AcknowledgementSetManager.class));
        assertThat(argumentsSuppliers.get(AcknowledgementSetManager.class), notNullValue());
        assertThat(argumentsSuppliers.get(AcknowledgementSetManager.class).get(), equalTo(acknowledgementSetManager));

        assertThat(argumentsSuppliers, hasKey(CircuitBreaker.class));
        assertThat(argumentsSuppliers.get(CircuitBreaker.class), notNullValue());
        assertThat(argumentsSuppliers.get(CircuitBreaker.class).get(), equalTo(circuitBreaker));
    }

    @Test
    void getArgumentsSuppliers_returns_map_with_null_optional_CircuitBreaker() {
        circuitBreaker = null;

        final Map<Class<?>, Supplier<Object>> argumentsSuppliers = createObjectUnderTest().getArgumentsSuppliers();

        assertThat(argumentsSuppliers.size(), equalTo(3));

        assertThat(argumentsSuppliers, hasKey(EventFactory.class));
        assertThat(argumentsSuppliers.get(EventFactory.class), notNullValue());
        assertThat(argumentsSuppliers.get(EventFactory.class).get(), equalTo(eventFactory));

        assertThat(argumentsSuppliers, hasKey(AcknowledgementSetManager.class));
        assertThat(argumentsSuppliers.get(AcknowledgementSetManager.class), notNullValue());
        assertThat(argumentsSuppliers.get(AcknowledgementSetManager.class).get(), equalTo(acknowledgementSetManager));

        assertThat(argumentsSuppliers, hasKey(CircuitBreaker.class));
        assertThat(argumentsSuppliers.get(CircuitBreaker.class), notNullValue());
        assertThat(argumentsSuppliers.get(CircuitBreaker.class).get(), nullValue());
    }
}