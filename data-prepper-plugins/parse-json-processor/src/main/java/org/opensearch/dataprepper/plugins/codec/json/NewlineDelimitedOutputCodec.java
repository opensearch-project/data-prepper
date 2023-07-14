/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name = "ndjson", pluginType = OutputCodec.class, pluginConfigurationType = NewlineDelimitedOutputConfig.class)
public class NewlineDelimitedOutputCodec implements OutputCodec {
    private static final String NDJSON = "ndjson";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final NewlineDelimitedOutputConfig config;

    @DataPrepperPluginConstructor
    public NewlineDelimitedOutputCodec(final NewlineDelimitedOutputConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        Objects.requireNonNull(event);
        Map<String, Object> eventMap;
        if (tagsTargetKey != null) {
            eventMap = addTagsToEvent(event, tagsTargetKey).toMap();
        } else {
            eventMap = event.toMap();
        }
        writeToOutputStream(outputStream, eventMap);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    private void writeToOutputStream(final OutputStream outputStream, final Object object) throws IOException {
        byte[] byteArr = null;
        if (object instanceof Map) {
            Map<Object, Object> map = objectMapper.convertValue(object, Map.class);
            for (String key : config.getExcludeKeys()) {
                if (map.containsKey(key)) {
                    map.remove(key);
                }
            }
            String json = objectMapper.writeValueAsString(map);
            byteArr = json.getBytes();
        } else {
            byteArr = object.toString().getBytes();
        }
        outputStream.write(byteArr);
        outputStream.write(System.lineSeparator().getBytes());
    }

    @Override
    public String getExtension() {
        return NDJSON;
    }
}
