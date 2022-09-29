/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class RouteEventEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(RouteEventEvaluator.class);

    private final ExpressionEvaluator<Boolean> evaluator;
    private final Collection<ConditionalRoute> routes;

    RouteEventEvaluator(final ExpressionEvaluator<Boolean> evaluator, final Collection<ConditionalRoute> routes) {
        this.evaluator = evaluator;
        this.routes = routes;
    }

    Map<Record, Set<String>> evaluateEventRoutes(final Collection<Record> records) {
        final Map<Record, Set<String>> recordsToRoutes = new HashMap<>();

        int nonEventRecords = 0;

        for (Record record : records) {

            final Object data = record.getData();

            if(data instanceof Event) {

                final Event event = (Event) data;

                recordsToRoutes.put(record, new HashSet<>());

                for (ConditionalRoute route : routes) {
                    Boolean routed;
                    try {
                        routed = evaluator.evaluate(route.getCondition(), event);
                    } catch (final Exception ex) {
                        routed = false;
                        LOG.error("Failed to evaluate route. This route will not be applied to any events.", ex);
                    }
                    if (routed) {
                        recordsToRoutes
                                .get(record)
                                .add(route.getName());
                    }
                }
            } else {
                nonEventRecords++;
                recordsToRoutes.put(record, Collections.emptySet());
            }
        }

        if(nonEventRecords > 0) {
            LOG.warn("Received {} records which are not events. These will have no routes applied.", nonEventRecords);
        }

        return recordsToRoutes;
    }
}
