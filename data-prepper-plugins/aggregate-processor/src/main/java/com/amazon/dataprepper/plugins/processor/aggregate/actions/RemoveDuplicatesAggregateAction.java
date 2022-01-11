/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate.actions;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionResponse;

import java.util.Map;

/**
 * An AggregateAction that will pass down the first Event of a groupState immediately for processing, and then ignore Events
 * that have a non-empty groupState associated with them
 * @since 1.3
 */
@DataPrepperPlugin(name = "remove_duplicates", pluginType = AggregateAction.class)
public class RemoveDuplicatesAggregateAction implements AggregateAction {
    @Override
    public AggregateActionResponse handleEvent(final Event event, final Map<Object, Object> groupState) {
        if (groupState.size() == 0) {
            groupState.putAll(event.toMap());
            return AggregateActionResponse.fromEvent(event);
        }

        return AggregateActionResponse.emptyEventResponse();
    }
}
