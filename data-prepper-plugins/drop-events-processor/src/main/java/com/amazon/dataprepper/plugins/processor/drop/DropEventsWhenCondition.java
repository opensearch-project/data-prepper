/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.event.Event;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DropEventsWhenCondition {
    private static final Logger LOG = LoggerFactory.getLogger(DropEventsWhenCondition.class);
    private static final HandleFailedEventsOption DEFAULT_HANDLE_FAILED_EVENTS = HandleFailedEventsOption.skip;

    @Nullable
    private final String whenSetting;
    private final HandleFailedEventsOption handleFailedEventsSetting;
    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    public DropEventsWhenCondition(
            @Nullable final Object whenSetting,
            @Nullable final Object handleFailedEventsSetting,
            final ExpressionEvaluator<Boolean> expressionEvaluator
    ) {
        this.expressionEvaluator = expressionEvaluator;

        if (whenSetting instanceof String) {
            this.whenSetting = (String) whenSetting;
        }
        else if (whenSetting == null) {
            this.whenSetting = null;
        }
        else {
            throw new IllegalArgumentException("Expected drop_events.when to be of type String");
        }

        if (handleFailedEventsSetting instanceof String) {
            this.handleFailedEventsSetting = HandleFailedEventsOption.valueOf((String) handleFailedEventsSetting);
        }
        else if (handleFailedEventsSetting == null) {
            this.handleFailedEventsSetting = DEFAULT_HANDLE_FAILED_EVENTS;
        }
        else {
            throw new IllegalArgumentException("Expected drop_events.handle_failed_events to be of type String");
        }
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
}
