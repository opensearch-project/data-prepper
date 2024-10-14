/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.opensearch.dataprepper.core.parser.DataFlowComponent;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Provides routing of event records over a collection of {@link DataFlowComponent} objects.
 */
public class Router {
    private final RouteEventEvaluator routeEventEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;
    private final Consumer<Event> noRouteHandler;

    Router(final RouteEventEvaluator routeEventEvaluator, final DataFlowComponentRouter dataFlowComponentRouter, final Consumer<Event> noRouteHandler) {
        this.routeEventEvaluator = Objects.requireNonNull(routeEventEvaluator);
        this.dataFlowComponentRouter = dataFlowComponentRouter;
        this.noRouteHandler = noRouteHandler;
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

        boolean allRecordsRouted = false;

        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            if (dataFlowComponent.getRoutes().isEmpty()) {
                allRecordsRouted = true;
                break;
            }
        }

        final Set<Record> recordsUnRouted = (allRecordsRouted) ? null : new HashSet<>(allRecords);

        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            dataFlowComponentRouter.route(allRecords, dataFlowComponent, recordsToRoutes, getRecordStrategy, (component, records) -> { 
                if (recordsUnRouted != null) {
                    for (final Record record: records) {
                        recordsUnRouted.remove(record);
                    }
                }
                componentRecordsConsumer.accept(component, records);
            });
        }

        if (recordsUnRouted != null) {
            for (Record record: recordsUnRouted) {
                if (record.getData() instanceof Event) {
                    noRouteHandler.accept((Event)record.getData());
                }
            }
        }
    }
}
