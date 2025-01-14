package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import java.util.Collections;
import java.util.List;

public class StandardMessageFieldStrategy implements MessageFieldStrategy {
    @Override
    public List<Event> parseEvents(final String messageBody) {
        final Event event = JacksonEvent.builder()
                .withEventType("DOCUMENT")
                .withData(Collections.singletonMap("message", messageBody))
                .build();
        return Collections.singletonList(event);
    }
}
