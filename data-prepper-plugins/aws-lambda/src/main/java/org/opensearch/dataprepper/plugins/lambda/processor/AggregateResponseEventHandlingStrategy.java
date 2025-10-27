/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.ResponseEventHandlingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class AggregateResponseEventHandlingStrategy implements ResponseEventHandlingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateResponseEventHandlingStrategy.class);

    @Override
    public List<Record<Event>> handleEvents(List<Event> parsedEvents, List<Record<Event>> originalRecords,
                                            BiConsumer<Event, Event> consumerUnused) {

        List<Record<Event>> resultRecords = new ArrayList<>();
        Event originalEvent = originalRecords.get(0).getData();
        EventHandle handle = originalEvent.getEventHandle();

        for (Event responseEvent : parsedEvents) {
            Record<Event> newRecord = new Record<>(responseEvent);
            resultRecords.add(newRecord);
        }

        if (handle instanceof DefaultEventHandle) {
            DefaultEventHandle eventHandle = (DefaultEventHandle) handle;
            AcknowledgementSet originalAcknowledgementSet = eventHandle.getAcknowledgementSet();
            
            for (Event responseEvent : parsedEvents) {
                if (originalAcknowledgementSet != null) {
                    originalAcknowledgementSet.add(responseEvent);
                }
            }
        } else if (handle instanceof AggregateEventHandle) {
            AggregateEventHandle aggregateHandle = (AggregateEventHandle) handle;
            
            for (Event responseEvent : parsedEvents) {
                aggregateHandle.addEventHandle(responseEvent.getEventHandle());
            }
        } else {
            LOG.warn("Unsupported event handle type: {}. Events will not be acknowledged.", handle.getClass().getName());
        }

        return resultRecords;
    }
}
