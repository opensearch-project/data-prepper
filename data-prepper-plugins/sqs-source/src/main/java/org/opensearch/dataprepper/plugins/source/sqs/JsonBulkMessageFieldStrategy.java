package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class JsonBulkMessageFieldStrategy implements MessageFieldStrategy {

    private final InputCodec codec;

    public JsonBulkMessageFieldStrategy(final InputCodec codec) {
        this.codec = codec;
    }

    @Override
    public List<Event> parseEvents(final String messageBody) {
        final List<Event> events = new ArrayList<>();
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(messageBody.getBytes(StandardCharsets.UTF_8));
        try {
            codec.parse(inputStream, (Consumer<Record<Event>>) record -> events.add(record.getData()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse events from SQS body.", e);
        }
        return events;
    }
}
