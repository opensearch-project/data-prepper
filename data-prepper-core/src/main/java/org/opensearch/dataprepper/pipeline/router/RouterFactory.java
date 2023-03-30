/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import java.util.Objects;
import java.util.Set;

public class RouterFactory {
    private final ExpressionEvaluator<Boolean> expressionEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final EventFactory eventFactory;

    RouterFactory(final ExpressionEvaluator<Boolean> expressionEvaluator, final EventFactory eventFactory, final AcknowledgementSetManager acknowledgementSetManager) {
        this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator);
        this.acknowledgementSetManager = Objects.requireNonNull(acknowledgementSetManager);
        this.eventFactory = Objects.requireNonNull(eventFactory);

        dataFlowComponentRouter = new DataFlowComponentRouter();
    }

    public Router createRouter(final Set<ConditionalRoute> routes) {
        final RouteEventEvaluator routeEventEvaluator = new RouteEventEvaluator(expressionEvaluator, routes);
        return new Router(routeEventEvaluator, dataFlowComponentRouter, eventFactory, acknowledgementSetManager);
    }
}
