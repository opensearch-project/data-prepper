/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.newlineinputcodec;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "Newline", pluginType = InputCodec.class, pluginConfigurationType = NewlineDelimitedInputConfig.class)
public class NewlineDelimitedInputCodec implements InputCodec {
    private static final String MESSAGE_FIELD_NAME = "message";
    private final int skipLines;
    private final String headerDestination;

    @DataPrepperPluginConstructor
    public NewlineDelimitedInputCodec(final NewlineDelimitedInputConfig config) {
        Objects.requireNonNull(config);
        skipLines = config.getSkipLines();

        if (skipLines < 0) {
            throw new IllegalArgumentException("skipLines must be non-negative.");
        }

        headerDestination = config.getHeaderDestination();
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            parseBufferedReader(reader, eventConsumer);
        }
    }

    private void parseBufferedReader(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final boolean doAddHeaderToOutgoingEvents = Objects.nonNull(headerDestination);
        boolean hasReadHeader = false;
        String header = "";

        int linesToSkip = skipLines;
        String line;
        while ((line = reader.readLine()) != null) {
            final boolean shouldSkipBecauseThisLineIsHeader = doAddHeaderToOutgoingEvents && !hasReadHeader;
            final boolean shouldSkipThisLine = linesToSkip > 0 || shouldSkipBecauseThisLineIsHeader;

            if (shouldSkipThisLine) {
                if (linesToSkip > 0) {
                    linesToSkip--;
                } else {
                    header = line;
                    hasReadHeader = true;
                }
                continue;
            }

            final Map<String, String> eventData = new HashMap<>();

            if (doAddHeaderToOutgoingEvents) {
                eventData.put(headerDestination, header);
            }
            eventData.put(MESSAGE_FIELD_NAME, line);

            final Event event = JacksonLog.builder().withData(eventData).build();
            eventConsumer.accept(new Record<>(event));
        }
    }
}
