/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as ND-JSON data
 */
@DataPrepperPlugin(name = "ndjson", pluginType = OutputCodec.class, pluginConfigurationType = NdjsonOutputConfig.class)
public class NdjsonOutputCodec implements OutputCodec {
    private static final String NDJSON = "ndjson";
    private static final String JSONL = "jsonl";
    private static final Set<String> VALID_EXTENSIONS = Set.of(NDJSON, JSONL);

    private final NdjsonOutputConfig ndjsonOutputConfig;
    private OutputCodecContext deprecatedSupportCodecContext;

    @DataPrepperPluginConstructor
    public NdjsonOutputCodec(final NdjsonOutputConfig config) {
        Objects.requireNonNull(config, "NdjsonOutputConfig cannot be null");
        // Validate extension
        String configExtension = config.getExtension();
        if (configExtension != null && !VALID_EXTENSIONS.contains(configExtension.toLowerCase())) {
            throw new IllegalArgumentException(
                    String.format("Invalid extension '%s'. Allowed values are: %s",
                            configExtension,
                            String.join(", ", VALID_EXTENSIONS))
            );
        }

        this.ndjsonOutputConfig = config;
    }

    private static class NdjsonWriter implements Writer {
        private final OutputStream outputStream;
        private final OutputCodecContext codecContext;

        private NdjsonWriter(final OutputStream outputStream, final OutputCodecContext codecContext) {
            this.outputStream = outputStream;
            this.codecContext = codecContext;
        }

        @Override
        public void writeEvent(final Event event) throws IOException {
            doWriteEvent(outputStream, event, codecContext);
        }

        @Override
        public void complete() throws IOException {
            outputStream.close();
        }
    }

    @Override
    public Writer createWriter(final OutputStream outputStream, final Event sampleEvent, final OutputCodecContext codecContext) {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);

        return new NdjsonWriter(outputStream, codecContext);
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.deprecatedSupportCodecContext = codecContext;
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);

        String json = event.jsonBuilder()
                .includeKeys(deprecatedSupportCodecContext.getIncludeKeys())
                .excludeKeys(deprecatedSupportCodecContext.getExcludeKeys())
                .includeTags(deprecatedSupportCodecContext.getTagsTargetKey())
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
        if (ndjsonOutputConfig.getExtension() == null) {
            return NDJSON;
        }
        return ndjsonOutputConfig.getExtension();
    }

    private static void doWriteEvent(final OutputStream outputStream, final Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(event);

        String json = event.jsonBuilder()
                .includeKeys(codecContext.getIncludeKeys())
                .excludeKeys(codecContext.getExcludeKeys())
                .includeTags(codecContext.getTagsTargetKey())
                .toJsonString();
        outputStream.write(json.getBytes());
        outputStream.write(System.lineSeparator().getBytes());
    }
}
