/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ActionConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamDetector;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamIndex;

import java.util.List;

public class EventActionResolver {

    private final String defaultAction;
    private final List<ActionConfiguration> actions;
    private final ExpressionEvaluator expressionEvaluator;
    private DataStreamDetector dataStreamDetector;
    private DataStreamIndex dataStreamIndex;

    public EventActionResolver(final String defaultAction,
                               final List<ActionConfiguration> actions,
                               final ExpressionEvaluator expressionEvaluator) {
        this.defaultAction = defaultAction;
        this.actions = actions;
        this.expressionEvaluator = expressionEvaluator;
    }

    public void setDataStreamSupport(final DataStreamDetector dataStreamDetector,
                                     final DataStreamIndex dataStreamIndex) {
        this.dataStreamDetector = dataStreamDetector;
        this.dataStreamIndex = dataStreamIndex;
    }

    String resolveAction(final Event event, final String indexName) {
        String eventAction = defaultAction;
        if (actions != null) {
            for (final ActionConfiguration actionEntry : actions) {
                final String condition = actionEntry.getWhen();
                eventAction = actionEntry.getType();
                if (condition != null &&
                        expressionEvaluator.evaluateConditional(condition, event)) {
                    break;
                }
            }
        }
        if (eventAction.contains("${")) {
            eventAction = event.formatString(eventAction, expressionEvaluator);
        }

        if (dataStreamDetector != null && dataStreamDetector.isDataStream(indexName)) {
            eventAction = dataStreamIndex.determineAction(eventAction, indexName);
        }

        return eventAction;
    }

    public boolean isValidAction(final String action) {
        return OpenSearchBulkActions.fromOptionValue(action) != null;
    }
}
