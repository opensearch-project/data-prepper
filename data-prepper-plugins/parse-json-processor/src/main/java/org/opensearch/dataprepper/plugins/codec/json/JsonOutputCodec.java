/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as JSON Data
 */
@DataPrepperPlugin(name = "json", pluginType = OutputCodec.class, pluginConfigurationType = JsonOutputCodecConfig.class)
public class JsonOutputCodec implements OutputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String JSON = "json";
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private final JsonOutputCodecConfig config;
    private JsonWriter deprecatedSupportWriter;

    @DataPrepperPluginConstructor
    public JsonOutputCodec(final JsonOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    private class JsonWriter implements Writer {
        private final JsonGenerator generator;
        private final OutputStream outputStream;
        private final OutputCodecContext codecContext;

        private JsonWriter(final OutputStream outputStream, final OutputCodecContext codecContext) throws IOException {
            this.outputStream = outputStream;
            this.codecContext = codecContext;
            generator = JSON_FACTORY.createGenerator(outputStream, JsonEncoding.UTF8);
            generator.writeStartObject();
            generator.writeFieldName(config.getKeyName());
            generator.writeStartArray();
        }

        @Override
        public void writeEvent(final Event event) throws IOException {
            Objects.requireNonNull(event);
            final Map<String, Object> dataMap = getDataMapToSerialize(event);
            objectMapper.writeValue(generator, dataMap);
            generator.flush();
        }

        @Override
        public void complete() throws IOException {
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();
            outputStream.flush();
            outputStream.close();
        }

        private Map<String, Object> getDataMapToSerialize(final Event event) throws JsonProcessingException {
            final Event modifiedEvent;
            if (codecContext.getTagsTargetKey() != null) {
                modifiedEvent = addTagsToEvent(event, codecContext.getTagsTargetKey());
            } else {
                modifiedEvent = event;
            }
            Map<String, Object> dataMap = modifiedEvent.toMap();

            if ((codecContext.getIncludeKeys() != null && !codecContext.getIncludeKeys().isEmpty()) ||
                    (codecContext.getExcludeKeys() != null && !codecContext.getExcludeKeys().isEmpty())) {

                final Map<String, Object> finalDataMap = dataMap;
                dataMap = dataMap.keySet()
                        .stream()
                        .filter(codecContext::shouldIncludeKey)
                        .collect(Collectors.toMap(Function.identity(), finalDataMap::get));
            }
            return dataMap;
        }
    }

    @Override
    public String getExtension() {
        return JSON;
    }

    @Override
    public Writer createWriter(final OutputStream outputStream, final Event sampleEvent, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);

        return new JsonWriter(outputStream, codecContext);
    }

    @Override
    public void start(final OutputStream outputStream, final Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        deprecatedSupportWriter = new JsonWriter(outputStream, codecContext);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        deprecatedSupportWriter.complete();
    }

    @Override
    public synchronized void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        deprecatedSupportWriter.writeEvent(event);
    }
}


