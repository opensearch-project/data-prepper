/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.opensearch.dataprepper.core.parser.DataFlowComponent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Package-protected utility to route for a single {@link DataFlowComponent}. This is
 * intended to help break apart {@link Router} for better testing.
 */
class DataFlowComponentRouter {
    static final String DEFAULT_ROUTE = "_default";
    <C> void route(final Collection<Record> allRecords,
                   final DataFlowComponent<C> dataFlowComponent,
                   final Map<Record, Set<String>> recordsToRoutes,
                   final RouterGetRecordStrategy getRecordStrategy,
                   final BiConsumer<C, Collection<Record>> componentRecordsConsumer) {

        final Collection<Record> recordsForComponent;
        final Set<String> dataFlowComponentRoutes =  dataFlowComponent.getRoutes();

        if (dataFlowComponentRoutes.isEmpty()) {
            recordsForComponent = getRecordStrategy.getAllRecords(allRecords);
        } else {
            recordsForComponent = new ArrayList<>();
            for (Record record : allRecords) {
                final Set<String> routesForEvent = recordsToRoutes
                        .getOrDefault(record, Collections.emptySet());

                if (routesForEvent.size() == 0 && dataFlowComponentRoutes.contains(DEFAULT_ROUTE)) {
                    recordsForComponent.add(getRecordStrategy.getRecord(record));
                } else if (routesForEvent.stream().anyMatch(dataFlowComponentRoutes::contains)) {
                    recordsForComponent.add(getRecordStrategy.getRecord(record));
                }
            }
        }
        componentRecordsConsumer.accept(dataFlowComponent.getComponent(), recordsForComponent);
    }
}
