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

    @DataPrepperPluginConstructor
    public JsonOutputCodec(final JsonOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream, Event event, String tagsTargetKey) throws IOException {
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
    public synchronized void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        Objects.requireNonNull(event);
        final Event modifiedEvent;
        if (tagsTargetKey != null) {
            modifiedEvent = addTagsToEvent(event, tagsTargetKey);
        } else {
            modifiedEvent = event;
        }
        generator.writeStartObject();
        final boolean isExcludeKeyAvailable = !Objects.isNull(config.getExcludeKeys());
        for (final String key : modifiedEvent.toMap().keySet()) {
            if (isExcludeKeyAvailable && config.getExcludeKeys().contains(key)) {
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


