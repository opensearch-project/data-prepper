/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.event.Event;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @since 1.3
 *
 * DropEventsWhenCondition tracks if valid when statement is configured and can evaluate a when statement using a given context.
 *
 */
class DropEventsWhenCondition {
    private static final Logger LOG = LoggerFactory.getLogger(DropEventsWhenCondition.class);
    private static final HandleFailedEventsOption DEFAULT_HANDLE_FAILED_EVENTS = HandleFailedEventsOption.SKIP;
    private static final String SHOULD_SKIP_EVALUATING_CONDITIONAL = "true";

    private final String whenSetting;
    private final HandleFailedEventsOption handleFailedEventsSetting;
    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    DropEventsWhenCondition(final Builder builder) {
        whenSetting = builder.whenSetting;
        handleFailedEventsSetting = builder.handleFailedEventsSetting;
        expressionEvaluator = builder.expressionEvaluator;
    }

    /**
     * @since 1.3
     *
     * Check if evaluator should be used. On false all records can be dropped.
     *
     * @return if {@link DropEventsWhenCondition#isStatementFalseWith(Event)} should be used
     */
    public boolean shouldEvaluateConditional() {
        return !Objects.equals(whenSetting, SHOULD_SKIP_EVALUATING_CONDITIONAL);
    }

    /**
     * @since 1.3
     *
     * Checks if {@link DropEventsProcessor} when condition evaluates to false using event to resolve Json Pointers
     *
     * @param event Source for resolving Json Pointers
     * @return if the event should proceed to next {@link com.amazon.dataprepper.model.processor.Processor}
     */
    public boolean isStatementFalseWith(final Event event) {
        try {
            return expressionEvaluator.evaluate(whenSetting, event);
        } catch (final Exception e) {
            switch (handleFailedEventsSetting) {
                case SKIP:
                    LOG.warn("An exception occurred while processing when expression for event {}", event, e);
                    return false;
                case SKIP_SILENTLY:
                    return false;
                case DROP:
                    LOG.warn("An exception occurred resulting in a dropped event while processing when expression for event {}", event, e);
                    return true;
                case DROP_SILENTLY:
                    return true;
                default:
                    final String exceptionMessage = "Data Prepper internal exception, unhandled value " + handleFailedEventsSetting;
                    throw new IllegalStateException(exceptionMessage, e);
            }
        }
    }

    /**
     * @since 1.3
     *
     * Builder for creating {@link DropEventsWhenCondition}
     */
    static class Builder {
        private String whenSetting;
        private HandleFailedEventsOption handleFailedEventsSetting = DEFAULT_HANDLE_FAILED_EVENTS;

        private ExpressionEvaluator<Boolean> expressionEvaluator;

        public Builder withDropEventsProcessorConfig(final DropEventProcessorConfig dropEventProcessorConfig) {
            this.whenSetting = dropEventProcessorConfig.getWhen();
            this.handleFailedEventsSetting = Objects.requireNonNull(dropEventProcessorConfig.getHandleFailedEventsOption());
            return this;
        }

        public Builder withExpressionEvaluator(final ExpressionEvaluator<Boolean> expressionEvaluator) {
            this.expressionEvaluator = expressionEvaluator;
            return this;
        }

        public DropEventsWhenCondition build() {
            if (whenSetting == null) {
                throw new IllegalArgumentException(
                        "Starting in Data Prepper v1.3.0 drop_events processor requires \"when\" option to be specified. See " +
                        "https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/drop-events-processor/README.md " +
                        "for more information"
                );
            }
            if (!Objects.equals(whenSetting, SHOULD_SKIP_EVALUATING_CONDITIONAL) && expressionEvaluator == null) {
                throw new IllegalStateException("Use of drop events processor when setting requires a ExpressionEvaluator bean at runtime");
            }
            return new DropEventsWhenCondition(this);
        }
    }
}
