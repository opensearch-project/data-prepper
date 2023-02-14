package org.opensearch.dataprepper.jsonCodec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "json", pluginType = InputCodec.class)
public class JsonInputCodec implements InputCodec {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);

        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                parseRecordsArray(jsonParser, eventConsumer);
            }
        }

    }

    private void parseRecordsArray(final JsonParser jsonParser, final Consumer<Record<Event>> eventConsumer) throws IOException {
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

            final Record<Event> record = createRecord(innerJson);
            eventConsumer.accept(record);
        }
    }

    private Record<Event> createRecord(final Map<String, Object> json) {
        final JacksonEvent event = JacksonLog.builder()
                .withData(json)
                .build();

        return new Record<>(event);
    }

}
