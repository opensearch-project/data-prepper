package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.List;

public interface ResponseEventHandlingStrategy {
    void handleEvents(List<Event> parsedEvents, List<Record<Event>> originalRecords,
                      List<Record<Event>> resultRecords);
}
