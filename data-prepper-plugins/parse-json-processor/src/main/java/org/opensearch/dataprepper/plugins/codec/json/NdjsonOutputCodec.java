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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name = "ndjson", pluginType = OutputCodec.class, pluginConfigurationType = NdjsonOutputConfig.class)
public class NdjsonOutputCodec implements OutputCodec {
    private static final String NDJSON = "ndjson";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final NdjsonOutputConfig config;

    private final List<String> includeKeys;
    private final List<String> excludeKeys;


    @DataPrepperPluginConstructor
    public NdjsonOutputCodec(final NdjsonOutputConfig config) {
        Objects.requireNonNull(config);
        this.config = config;

        this.includeKeys = preprocessingKeys(config.getIncludeKeys());
        this.excludeKeys = preprocessingKeys(config.getExcludeKeys());
    }

    @Override
    public void start(final OutputStream outputStream, Event event, String tagsTargetKey) throws IOException {
        Objects.requireNonNull(outputStream);
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream, String tagsTargetKey) throws IOException {
        Objects.requireNonNull(event);
        String jsonString = event.jsonBuilder().includeKeys(includeKeys).excludeKeys(excludeKeys).includeTags(tagsTargetKey).toJsonString();

        outputStream.write(jsonString.getBytes());
        outputStream.write(System.lineSeparator().getBytes());
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    @Override
    public String getExtension() {
        return NDJSON;
    }

    /**
     * Pre-processes a list of Keys and returns a sorted list
     * The keys must start with `/` and not end with `/`
     *
     * @param keys a list of raw keys
     * @return a sorted processed keys
     */
    private List<String> preprocessingKeys(final List<String> keys) {
        if (keys.contains("/")) {
            return new ArrayList<>();
        }
        List<String> result = keys.stream()
                .map(k -> k.startsWith("/") ? k : "/" + k)
                .map(k -> k.endsWith("/") ? k.substring(0, k.length() - 1) : k)
                .collect(Collectors.toList());
        Collections.sort(result);
        return result;
    }

}
