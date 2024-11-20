/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.ResponseEventHandlingStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StrictResponseEventHandlingStrategy implements ResponseEventHandlingStrategy {

    @Override
    public List<Record<Event>> handleEvents(List<Event> parsedEvents,
                                            List<Record<Event>> originalRecords) {
        if (parsedEvents.size() != originalRecords.size()) {
            throw new RuntimeException(
                    "Response Processing Mode is configured as Strict mode but behavior is aggregate mode. Event count mismatch.");
        }

        List<Record<Event>> resultRecords = new ArrayList<>();
        for (int i = 0; i < parsedEvents.size(); i++) {
            Event responseEvent = parsedEvents.get(i);
            Event originalEvent = originalRecords.get(i).getData();

            // Clear the original event's data
            originalEvent.clear();

            // Manually copy each key-value pair from the responseEvent to the originalEvent
            Map<String, Object> responseData = responseEvent.toMap();
            for (Map.Entry<String, Object> entry : responseData.entrySet()) {
                originalEvent.put(entry.getKey(), entry.getValue());
            }

            // Add updated event to resultRecords
            resultRecords.add(originalRecords.get(i));
        }
        return resultRecords;
    }
}

