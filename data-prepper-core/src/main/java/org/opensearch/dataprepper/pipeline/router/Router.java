/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.parser.DataFlowComponent;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Provides routing of event records over a collection of {@link DataFlowComponent} objects.
 */
public class Router {
    private final RouteEventEvaluator routeEventEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;
    private final Consumer<Event> unroutedEventHandler;

    Router(final RouteEventEvaluator routeEventEvaluator, final DataFlowComponentRouter dataFlowComponentRouter, final Consumer<Event> handler) {
        this.routeEventEvaluator = Objects.requireNonNull(routeEventEvaluator);
        this.dataFlowComponentRouter = dataFlowComponentRouter;
        this.unroutedEventHandler = handler;
    }

    public <C> void route(
            final Collection<Record> allRecords,
            final Collection<DataFlowComponent<C>> dataFlowComponents,
            final RouterGetRecordStrategy getRecordStrategy,
            final BiConsumer<C, Collection<Record>> componentRecordsConsumer) {

        Objects.requireNonNull(allRecords);
        Objects.requireNonNull(dataFlowComponents);
        Objects.requireNonNull(componentRecordsConsumer);

        final Map<Record, Set<String>> recordsToRoutes = routeEventEvaluator.evaluateEventRoutes(allRecords);
        for (Map.Entry<Record, Set<String>> entry : recordsToRoutes.entrySet()) {
            if (entry.getValue().size() == 0) {
                Record record = entry.getKey();
                if (record.getData() instanceof Event) {
                    unroutedEventHandler.accept((Event)record.getData());
                }
            }
        }

        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            dataFlowComponentRouter.route(allRecords, dataFlowComponent, recordsToRoutes, getRecordStrategy, componentRecordsConsumer);
        }
    }
}
