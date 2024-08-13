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
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name = "ndjson", pluginType = OutputCodec.class, pluginConfigurationType = NdjsonOutputConfig.class)
public class NdjsonOutputCodec implements OutputCodec {
    private static final String NDJSON = "ndjson";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final NdjsonOutputConfig config;
    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public NdjsonOutputCodec(final NdjsonOutputConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.codecContext = codecContext;
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);

        String json = event.jsonBuilder()
                .includeKeys(codecContext.getIncludeKeys())
                .excludeKeys(codecContext.getExcludeKeys())
                .includeTags(codecContext.getTagsTargetKey())
                .toJsonString();
        outputStream.write(json.getBytes());
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
}
