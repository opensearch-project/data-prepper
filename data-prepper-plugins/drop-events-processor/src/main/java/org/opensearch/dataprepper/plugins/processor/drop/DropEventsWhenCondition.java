/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

/**
 * @since 1.3
 *
 * DropEventsWhenCondition tracks if valid when statement is configured and can evaluate a when statement using a given context.
 *
 */
class DropEventsWhenCondition {
    private static final Logger LOG = LoggerFactory.getLogger(DropEventsWhenCondition.class);
    private static final String HARDCODED_TRUE = "true";

    private final String dropWhen;
    private final HandleFailedEventsOption handleFailedEventsSetting;
    private final ExpressionEvaluator expressionEvaluator;
    private final boolean notAlwaysTrue;

    DropEventsWhenCondition(final Builder builder) {
        dropWhen = builder.dropWhen;
        handleFailedEventsSetting = builder.handleFailedEventsSetting;
        expressionEvaluator = builder.expressionEvaluator;
        notAlwaysTrue = !Objects.equals(dropWhen, HARDCODED_TRUE);
    }

    /**
     * @since 1.3
     *
     * Check if evaluator should be used. On false all records can be dropped.
     *
     * @return if {@link DropEventsWhenCondition#isStatementFalseWith(Event)} should be used
     */
    public boolean isNotAlwaysTrue() {
        return notAlwaysTrue;
    }

    /**
     * @since 1.3
     *
     * Checks if {@link DropEventsProcessor} when condition evaluates to false using event to resolve Json Pointers
     *
     * @param event Source for resolving Json Pointers
     * @return if the event should proceed to next {@link org.opensearch.dataprepper.model.processor.Processor}
     */
    public boolean isStatementFalseWith(final Event event) {
        try {
            return !expressionEvaluator.evaluateConditional(dropWhen, event);
        } catch (final Exception e) {
            if (handleFailedEventsSetting.shouldLog()) {
                LOG.warn(EVENT, "An exception occurred while processing when expression for event [{}]", event, e);
            }
            return handleFailedEventsSetting.shouldDropEvent();
        }
    }

    /**
     * @since 1.3
     *
     * Builder for creating {@link DropEventsWhenCondition}
     */
    static class Builder {
        private String dropWhen;
        private HandleFailedEventsOption handleFailedEventsSetting;

        private ExpressionEvaluator expressionEvaluator;

        public Builder withDropEventsProcessorConfig(final DropEventProcessorConfig dropEventProcessorConfig) {
            this.dropWhen = Objects.requireNonNull(dropEventProcessorConfig.getDropWhen());
            this.handleFailedEventsSetting = Objects.requireNonNull(dropEventProcessorConfig.getHandleFailedEventsOption());
            return this;
        }

        public Builder withExpressionEvaluator(final ExpressionEvaluator expressionEvaluator) {
            this.expressionEvaluator = expressionEvaluator;
            return this;
        }

        public DropEventsWhenCondition build() {
            if (dropWhen == null) {
                throw new IllegalArgumentException(
                        "Starting in Data Prepper v1.3.0 drop_events processor requires \"when\" option to be specified. See " +
                        "https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/drop-events-processor/README.md " +
                        "for more information"
                );
            }
            if (!Objects.equals(dropWhen, HARDCODED_TRUE) && expressionEvaluator == null) {
                throw new IllegalStateException("Use of drop events processor when setting requires a ExpressionEvaluator bean at runtime");
            }
            return new DropEventsWhenCondition(this);
        }
    }
}
