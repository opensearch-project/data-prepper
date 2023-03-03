/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.codec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Objects;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An implementation of {@link Codec} which serializes to JSON.
 */
@DataPrepperPlugin(name = "json", pluginType = Codec.class)
public class JsonCodec implements Codec {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Generates a serialized json string of the Events
     */

    @Override
    public void parse(final OutputStream outputStream, final Collection<Record<Event>> eventCollection) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(eventCollection);

        StringBuilder recordEventData = new StringBuilder();
        for (final Record<Event> recordEvent : eventCollection) {
            recordEventData.append(recordEvent.getData().toJsonString());

        }
        objectMapper.writeValue(outputStream, recordEventData.toString());
    }

    /**
     * Generates a serialized json string of the Events 
     */
    @Override
    public void parse(OutputStream outputStream, Record<Event> eventCollection) throws IOException
    {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(eventCollection);

        objectMapper.writeValue(outputStream, eventCollection.getData().toJsonString());

    }
    /*
     * Generates a serialized json string of the Event
     */
    @Override
    public void parse(OutputStream outputStream, Event event) throws IOException
    {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(event);

        objectMapper.writeValue(outputStream, event.toJsonString());
        
    }
}