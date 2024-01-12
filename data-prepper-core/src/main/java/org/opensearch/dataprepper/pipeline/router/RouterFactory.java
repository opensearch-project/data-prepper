/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Objects;
import java.util.Set;

public class RouterFactory {
    private final ExpressionEvaluator expressionEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;

    RouterFactory(final ExpressionEvaluator expressionEvaluator) {
        this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator);
        dataFlowComponentRouter = new DataFlowComponentRouter();
    }

    public Router createRouter(final Set<ConditionalRoute> routes) {
        final RouteEventEvaluator routeEventEvaluator = new RouteEventEvaluator(expressionEvaluator, routes);
        return new Router(routeEventEvaluator, dataFlowComponentRouter, event -> event.getEventHandle().release(true));
    }
}
