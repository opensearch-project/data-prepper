/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.parser.DataFlowComponent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.function.BiConsumer;

/**
 * Package-protected utility to route for a single {@link DataFlowComponent}. This is
 * intended to help break apart {@link Router} for better testing.
 */
class DataFlowComponentRouter {
    Record getRecordForComponent(Set<Record> usedRecords, Record record) {
        if (usedRecords == null) {
            return record;
        }
        if (!usedRecords.contains(record)) {
            usedRecords.add(record);
            return record;
        }
        if (record.getData() instanceof JacksonSpan) {
            try {
                final Span spanEvent = (Span) record.getData();
                Span newSpanEvent = JacksonSpan.fromSpan(spanEvent);
                return new Record<>(newSpanEvent);
            } catch (Exception ex) {
            }
        } else if (record.getData() instanceof Event) {
            try {
                final Event recordEvent = (Event) record.getData();
                Event newRecordEvent = JacksonEvent.fromEvent(recordEvent);
                return new Record<>(newRecordEvent);
            } catch (Exception ex) {
            }
        }
        return record;
    }

    <C> void route(final Collection<Record> allRecords,
                   final DataFlowComponent<C> dataFlowComponent,
                   final Map<Record, Set<String>> recordsToRoutes,
                   Set<Record> usedRecords,
                   final BiConsumer<C, Collection<Record>> componentRecordsConsumer) {

        final Collection<Record> recordsForComponent;
        final Set<String> dataFlowComponentRoutes =  dataFlowComponent.getRoutes();

        if (dataFlowComponentRoutes.isEmpty()) {
            if (usedRecords == null || usedRecords.isEmpty()) {
                recordsForComponent = allRecords;
                if (usedRecords != null) {
                    usedRecords.addAll(allRecords);
                }
            } else {
                recordsForComponent = new ArrayList<>();
                for (Record record : allRecords) {
                    recordsForComponent.add(getRecordForComponent(usedRecords, record));
                }
            }
        } else {
            recordsForComponent = new ArrayList<>();
            for (Record record : allRecords) {
                final Set<String> routesForEvent = recordsToRoutes
                        .getOrDefault(record, Collections.emptySet());

                if (routesForEvent.stream().anyMatch(dataFlowComponentRoutes::contains)) {
                    recordsForComponent.add(getRecordForComponent(usedRecords, record));
                }
            }
        }
        componentRecordsConsumer.accept(dataFlowComponent.getComponent(), recordsForComponent);
    }
}
