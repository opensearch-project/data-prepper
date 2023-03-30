/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class RouterFactoryTest {

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;
    private Set<ConditionalRoute> routes;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private EventFactory eventFactory;

    @BeforeEach
    void setUp() {
        final ConditionalRoute conditionalRoute = mock(ConditionalRoute.class);
        routes = Collections.singleton(conditionalRoute);
        eventFactory = mock(EventFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
    }

    private RouterFactory createObjectUnderTest() {
        return new RouterFactory(expressionEvaluator, eventFactory, acknowledgementSetManager);
    }

    @Test
    void constructor_throws_with_null_expressionEvaluator() {
        expressionEvaluator = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void createRouter_returns_new_Router() {
        final Router router = createObjectUnderTest().createRouter(routes);

        assertThat(router, notNullValue());
    }

    @Test
    void createRouter_returns_new_Router_with_empty_routes() {
        routes = Collections.emptySet();
        final Router router = createObjectUnderTest().createRouter(routes);

        assertThat(router, notNullValue());
    }
}
