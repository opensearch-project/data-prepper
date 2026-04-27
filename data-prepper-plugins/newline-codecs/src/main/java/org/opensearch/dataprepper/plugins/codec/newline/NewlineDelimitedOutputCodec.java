/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.codec.newline;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which writes the "message" field
 * from Data Prepper events as plain text, one message per line.
 * This matches Logstash's line format behavior.
 */
@DataPrepperPlugin(name = "newline", pluginType = OutputCodec.class, pluginConfigurationType = NewlineDelimitedOutputConfig.class)
public class NewlineDelimitedOutputCodec implements OutputCodec {
    private static final String NEWLINE = "txt";
    private static final String MESSAGE_FIELD = "message";
    @SuppressWarnings("unused")
    private OutputCodecContext deprecatedSupportCodecContext;
    private final boolean includeEmptyObjects;

    @DataPrepperPluginConstructor
    public NewlineDelimitedOutputCodec(final NewlineDelimitedOutputConfig config) {
        Objects.requireNonNull(config);
        this.includeEmptyObjects = config.isIncludeEmptyObjects();
    }

    private static class NewlineWriter implements Writer {
        private final OutputStream outputStream;
        private final OutputCodecContext codecContext;
        private final boolean includeEmptyObjects;

        private NewlineWriter(final OutputStream outputStream, final OutputCodecContext codecContext, final boolean includeEmptyObjects) {
            this.outputStream = outputStream;
            this.codecContext = codecContext;
            this.includeEmptyObjects = includeEmptyObjects;
        }

        @Override
        public void writeEvent(final Event event) throws IOException {
            doWriteEvent(outputStream, event, codecContext, includeEmptyObjects);
        }

        @Override
        public void complete() throws IOException {
            outputStream.close();
        }
    }

    @Override
    public Writer createWriter(final OutputStream outputStream, final Event sampleEvent, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);

        return new NewlineWriter(outputStream, codecContext, includeEmptyObjects);
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.deprecatedSupportCodecContext = codecContext;
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        doWriteEvent(outputStream, event, deprecatedSupportCodecContext, includeEmptyObjects);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    @Override
    public String getExtension() {
        return NEWLINE;
    }

    private static void doWriteEvent(final OutputStream outputStream, final Event event, final OutputCodecContext codecContext, final boolean includeEmptyObjects) throws IOException {
        Objects.requireNonNull(event);

        // Extract the message field and write it as plain text
        String message = null;
        if (event.containsKey(MESSAGE_FIELD)) {
            Object messageObj = event.get(MESSAGE_FIELD, Object.class);
            if (messageObj != null) {
                message = messageObj.toString();
            }
        }

        // Default: do not write anything if message is null or empty
        if (message == null || message.isEmpty()) {
            if (!includeEmptyObjects) {
                return;
            }
            message = "";
        }

        // Write the message as plain text followed by a newline
        outputStream.write(message.getBytes());
        outputStream.write(System.lineSeparator().getBytes());
    }
}