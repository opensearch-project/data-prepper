/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.Event;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
class RouterFactoryTest {

    @Mock
    private ExpressionEvaluator expressionEvaluator;
    private Set<ConditionalRoute> routes;
    private Consumer<Event> consumer;

    @BeforeEach
    void setUp() {
        final ConditionalRoute conditionalRoute = mock(ConditionalRoute.class);
        routes = Collections.singleton(conditionalRoute);
    }

    private RouterFactory createObjectUnderTest() {
        return new RouterFactory(expressionEvaluator);
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

    @Test
    void test_createRouterWithUnroutedHandler() {
        try (final MockedConstruction<Router> ignored =
                     mockConstruction(Router.class, (mock, context) -> {
                        consumer = (Consumer<Event>) context.arguments().get(2);
                     })) {
            Event event = mock(Event.class);
            EventHandle eventHandle = mock(EventHandle.class);
            when(event.getEventHandle()).thenReturn(eventHandle);
            final Router router = createObjectUnderTest().createRouter(routes);
            consumer.accept(event);
            verify(event.getEventHandle()).release(true);
            verify(eventHandle).release(true);
        }
    }
}
