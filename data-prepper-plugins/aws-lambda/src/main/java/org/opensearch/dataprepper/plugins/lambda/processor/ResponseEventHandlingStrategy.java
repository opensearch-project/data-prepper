package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;

import java.util.List;

public interface ResponseEventHandlingStrategy {
    void handleEvents(List<Event> parsedEvents, List<Record<Event>> originalRecords, List<Record<Event>> resultRecords, Buffer flushedBuffer);
}
