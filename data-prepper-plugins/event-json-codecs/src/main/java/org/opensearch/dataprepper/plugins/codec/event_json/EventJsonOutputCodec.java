/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "event_json", pluginType = OutputCodec.class, pluginConfigurationType = EventJsonOutputCodecConfig.class)
public class EventJsonOutputCodec implements OutputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    static final String EVENT_JSON = "event_json";
    private static final JsonFactory factory = new JsonFactory();
    private final EventJsonOutputCodecConfig config;
    private JsonGenerator generator;
    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public EventJsonOutputCodec(final EventJsonOutputCodecConfig config) {
        this.config = config;
    }

    @Override
    public String getExtension() {
        return EVENT_JSON;
    }

    @Override
    public void start(OutputStream outputStream, Event event, OutputCodecContext context) throws IOException {
        Objects.requireNonNull(outputStream);
        generator = factory.createGenerator(outputStream, JsonEncoding.UTF8);
        generator.writeStartArray();
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        generator.writeEndArray();
        generator.close();
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public synchronized void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        generator.writeStartObject();
        Objects.requireNonNull(event);
        getDataMapToSerialize(event);
        generator.flush();
        generator.writeEndObject();
    }

    private Map<String, Object> getDataMapToSerialize(Event event) throws IOException {
        Map<String, Object> dataMap = event.toMap();
        generator.writeFieldName(EventJsonDefines.DATA);
        objectMapper.writeValue(generator, dataMap);
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        generator.writeFieldName(EventJsonDefines.METADATA);
        objectMapper.writeValue(generator, metadataMap);
        return dataMap;
    }

}
