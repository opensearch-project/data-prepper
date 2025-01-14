package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;

public interface MessageFieldStrategy {
  //  Convert the SQS message body into one or more events.
    List<Event> parseEvents(String messageBody);
}
