/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class RouterFactory {
    private static final Consumer<Event> RELEASE_EVENT_ON_NO_ROUTE = event -> event.getEventHandle().release(true);
    private final ExpressionEvaluator expressionEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;

    RouterFactory(final ExpressionEvaluator expressionEvaluator) {
        this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator);
        dataFlowComponentRouter = new DataFlowComponentRouter();
    }

    public Router createRouter(final Set<ConditionalRoute> routes) {
        final RouteEventEvaluator routeEventEvaluator = new RouteEventEvaluator(expressionEvaluator, routes);
        return new Router(routeEventEvaluator, dataFlowComponentRouter,
                    RELEASE_EVENT_ON_NO_ROUTE);
    }
}
