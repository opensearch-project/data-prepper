/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.parser.DataFlowComponent;

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

    <C> void route(final Collection<Record> allRecords,
                   final DataFlowComponent<C> dataFlowComponent,
                   final Map<Record, Set<String>> recordsToRoutes,
                   final BiConsumer<C, Collection<Record>> componentRecordsConsumer) {

        final Collection<Record> recordsForComponent;
        if (dataFlowComponent.getRoutes().isEmpty()) {
            recordsForComponent = allRecords;
        } else {
            recordsForComponent = new ArrayList<>();
            for (Record event : allRecords) {
                final Set<String> routesForEvent = recordsToRoutes
                        .getOrDefault(event, Collections.emptySet());

                boolean routed = false;
                for (String route : dataFlowComponent.getRoutes()) {
                    if (routesForEvent.contains(route)) {
                        routed = true;
                        break;
                    }
                }

                if (routed) {
                    recordsForComponent.add(event);
                }
            }

        }
        componentRecordsConsumer.accept(dataFlowComponent.getComponent(), recordsForComponent);
    }
}
