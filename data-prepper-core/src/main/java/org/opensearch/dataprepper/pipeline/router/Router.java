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
import java.util.HashSet;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Provides routing of event records over a collection of {@link DataFlowComponent} objects.
 */
public class Router {
    private final RouteEventEvaluator routeEventEvaluator;
    private final DataFlowComponentRouter dataFlowComponentRouter;
    private final Consumer<Event> noRouteHandler;
    private Set<Record> recordsUnRouted;

    Router(final RouteEventEvaluator routeEventEvaluator, final DataFlowComponentRouter dataFlowComponentRouter, final Consumer<Event> noRouteHandler) {
        this.routeEventEvaluator = Objects.requireNonNull(routeEventEvaluator);
        this.dataFlowComponentRouter = dataFlowComponentRouter;
        this.noRouteHandler = noRouteHandler;
        this.recordsUnRouted = null;
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
        recordsUnRouted = null;

        boolean allRecordsRouted = false;

        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            if (dataFlowComponent.getRoutes().isEmpty()) {
                allRecordsRouted = true;
                break;
            }
        }

        if (!allRecordsRouted) {
            recordsUnRouted = new HashSet<>(allRecords);
        }

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
