/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.parser.DataFlowComponent;
import org.opensearch.dataprepper.pipeline.PipelineConnector;

import java.util.Collection;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Provides routing of event records over a collection of {@link DataFlowComponent} objects.
 */
public class Router {
    private final RouteEventEvaluator routeEventEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;

    Router(final RouteEventEvaluator routeEventEvaluator, final DataFlowComponentRouter dataFlowComponentRouter) {
        this.routeEventEvaluator = Objects.requireNonNull(routeEventEvaluator);
        this.dataFlowComponentRouter = dataFlowComponentRouter;
    }

    public <C> void route(
            final Collection<Record> allRecords,
            final Collection<DataFlowComponent<C>> dataFlowComponents,
            final BiConsumer<C, Collection<Record>> componentRecordsConsumer) {

        Objects.requireNonNull(allRecords);
        Objects.requireNonNull(dataFlowComponents);
        Objects.requireNonNull(componentRecordsConsumer);

        final Map<Record, Set<String>> recordsToRoutes = routeEventEvaluator.evaluateEventRoutes(allRecords);

        Set<Record> routedRecords = null;
        /*
         * If there are more than one sink and one of the sinks is
         * pipeline connector, then we should make a copy of every
         * record that is routed to more than one sink, so, to keep
         * track of already routed records, initialize the set.
         */
        if (dataFlowComponents.size() > 1) {
            for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
                if (dataFlowComponent.getComponent() instanceof PipelineConnector) {
                    routedRecords = new HashSet<Record>();
                    break;
                }
            }
        }
        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            dataFlowComponentRouter.route(allRecords, dataFlowComponent, recordsToRoutes, routedRecords, componentRecordsConsumer);
        }
    }
}
