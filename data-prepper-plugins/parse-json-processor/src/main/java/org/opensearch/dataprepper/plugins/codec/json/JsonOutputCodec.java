/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as JSON Data
 */
@DataPrepperPlugin(name = "json", pluginType = OutputCodec.class, pluginConfigurationType = JsonOutputCodecConfig.class)
public class JsonOutputCodec implements OutputCodec {

    private static final String JSON = "json";
    private static final JsonFactory factory = new JsonFactory();
    JsonOutputCodecConfig config;
    private JsonGenerator generator;
    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public JsonOutputCodec(final JsonOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.codecContext = codecContext;
        generator = factory.createGenerator(outputStream, JsonEncoding.UTF8);
        generator.writeStartObject();
        generator.writeFieldName(config.getKeyName());
        generator.writeStartArray();
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        generator.writeEndArray();
        generator.writeEndObject();
        generator.close();
        outputStream.flush();
        outputStream.close();
    }

    @Override
    public synchronized void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final Event modifiedEvent;
        if (codecContext.getTagsTargetKey() != null) {
            modifiedEvent = addTagsToEvent(event, codecContext.getTagsTargetKey());
        } else {
            modifiedEvent = event;
        }
        generator.writeStartObject();
        final boolean isExcludeKeyAvailable = !codecContext.getExcludeKeys().isEmpty();
        for (final String key : modifiedEvent.toMap().keySet()) {
            if (isExcludeKeyAvailable && codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
            generator.writeStringField(key, modifiedEvent.toMap().get(key).toString());
        }
        generator.writeEndObject();
        generator.flush();
    }

    @Override
    public String getExtension() {
        return JSON;
    }
}


