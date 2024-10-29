package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;

import java.util.List;

public class AggregateResponseEventHandlingStrategy implements ResponseEventHandlingStrategy {

    @Override
    public void handleEvents(List<Event> parsedEvents, List<Record<Event>> originalRecords, List<Record<Event>> resultRecords, Buffer flushedBuffer) {
        Event originalEvent = originalRecords.get(0).getData();
        DefaultEventHandle eventHandle = (DefaultEventHandle) originalEvent.getEventHandle();
        AcknowledgementSet originalAcknowledgementSet = eventHandle.getAcknowledgementSet();

        for (Event responseEvent : parsedEvents) {
            // Create a new record for the parsed response event
            Record<Event> newRecord = new Record<>(responseEvent);
            resultRecords.add(newRecord);

            // Add the new event to ack set, older event will be released by core later
            if (originalAcknowledgementSet != null) {
                originalAcknowledgementSet.add(responseEvent);
            }
        }
    }
}