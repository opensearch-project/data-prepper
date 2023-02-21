/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.parser.DataFlowComponent;

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

        Set<Record> usedRecords = new HashSet<Record>();
        for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
            if (!usedRecords.isEmpty()) {
                List<Record> newAllRecords = new ArrayList<Record>();
                for (Record record: allRecords) {
                    if (usedRecords.contains(record)) {
                        if (record.getData() instanceof JacksonSpan) {
                            try {
                                final Span spanEvent = (Span) record.getData();
                                Span newSpanEvent = JacksonSpan.fromSpan(spanEvent);
                                newAllRecords.add((new Record<>(newSpanEvent)));
                            } catch (Exception ex) {
                            }
                        } else if (record.getData() instanceof Event) {
                            try {
                                final Event recordEvent = (Event) record.getData();
                                Event newRecordEvent = JacksonEvent.fromEvent(recordEvent);
                                newAllRecords.add((new Record<>(newRecordEvent)));
                            } catch (Exception ex) {
                            }
                        }
                    } else {
                        newAllRecords.add(record);
                    }
                }
                usedRecords.addAll(dataFlowComponentRouter.route(newAllRecords, dataFlowComponent, recordsToRoutes, componentRecordsConsumer));
            } else {
                usedRecords.addAll(dataFlowComponentRouter.route(allRecords, dataFlowComponent, recordsToRoutes, componentRecordsConsumer));
            }
        }
    }
}
