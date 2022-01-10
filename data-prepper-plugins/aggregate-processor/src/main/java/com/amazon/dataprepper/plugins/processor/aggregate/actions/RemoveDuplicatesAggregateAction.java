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
import java.util.Optional;

@DataPrepperPlugin(name = "remove_duplicates", pluginType = AggregateAction.class)
public class RemoveDuplicatesAggregateAction implements AggregateAction {
    @Override
    public AggregateActionResponse handleEvent(Event event, Map<Object, Object> groupState) {
        if (groupState == null) {
            return new AggregateActionResponse(Optional.of(event), false);
        }

        return new AggregateActionResponse(Optional.empty(), false);
    }
}
