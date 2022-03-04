/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.event.Event;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DropEventsWhenCondition {
    private static final Logger LOG = LoggerFactory.getLogger(DropEventsWhenCondition.class);
    private static final HandleFailedEventsOption DEFAULT_HANDLE_FAILED_EVENTS = HandleFailedEventsOption.skip;

    private final String whenSetting;
    private final HandleFailedEventsOption handleFailedEventsSetting;
    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    DropEventsWhenCondition(final Builder builder) {
        whenSetting = builder.whenSetting;
        handleFailedEventsSetting = builder.handleFailedEventsSetting;
        expressionEvaluator = builder.expressionEvaluator;
    }

    public boolean shouldEvaluateConditional() {
        return whenSetting != null;
    }

    public boolean isStatementFalseWith(final Event event) {
        try {
            return expressionEvaluator.evaluate(whenSetting, event);
            // TODO Catch ExpressionEvaluationException
        } catch (final Exception e) {
            if (handleFailedEventsSetting == HandleFailedEventsOption.drop) {
                LOG.warn("An exception occurred resulting in a dropped event while processing when expression for event {}", event, e);
                return true;
            }
            else if (handleFailedEventsSetting == HandleFailedEventsOption.drop_silently) {
                return true;
            }
            else if (handleFailedEventsSetting == HandleFailedEventsOption.skip) {
                LOG.warn("An exception occurred while processing when expression for event {}", event, e);
                return false;
            }
            else if (handleFailedEventsSetting == HandleFailedEventsOption.skip_silently) {
                return false;
            }
            else {
                throw new IllegalStateException(
                        "Pipeline configuration drop_events.handle_failed_events contains invalid value " + handleFailedEventsSetting
                );
            }
        }
    }

    static class Builder {
        private String whenSetting;
        private HandleFailedEventsOption handleFailedEventsSetting = DEFAULT_HANDLE_FAILED_EVENTS;
        private ExpressionEvaluator<Boolean> expressionEvaluator;

        public Builder withWhenSetting(final Object whenSetting) {
            if (whenSetting instanceof String) {
                this.whenSetting = (String) whenSetting;
            }
            else if (whenSetting == null) {
                this.whenSetting = null;
            }
            else {
                throw new IllegalArgumentException("Expected drop_events.when to be of type String");
            }

            return this;
        }

        public Builder withHandleFailedEventsSetting(final Object handleFailedEventsSetting) {
            if (handleFailedEventsSetting instanceof String) {
                this.handleFailedEventsSetting = HandleFailedEventsOption.valueOf((String) handleFailedEventsSetting);
            }
            else if (handleFailedEventsSetting != null) {
                throw new IllegalArgumentException("Expected drop_events.handle_failed_events to be of type String");
            }
            return this;
        }

        public Builder withExpressionEvaluator(final ExpressionEvaluator<Boolean> expressionEvaluator) {
            this.expressionEvaluator = expressionEvaluator;
            return this;
        }

        public DropEventsWhenCondition build() {
            if (expressionEvaluator == null) {
                throw new IllegalStateException("Drop events processor requires valid expression evaluator");
            }
            return new DropEventsWhenCondition(this);
        }
    }
}
