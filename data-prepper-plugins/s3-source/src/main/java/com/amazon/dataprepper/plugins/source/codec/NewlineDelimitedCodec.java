/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.function.Consumer;

public class NewlineDelimitedCodec implements Codec {
    private final int skipLines;

    public NewlineDelimitedCodec(final NewlineDelimitedConfig config) {
        Objects.requireNonNull(config);
        skipLines = config.getSkipLines();

        if (skipLines < 0) {
            throw new IllegalArgumentException("skipLines must be non-negative.");
        }
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        parseBufferedReader(reader, eventConsumer);
    }

    private void parseBufferedReader(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        int linesToSkip = skipLines;
        String line;
        while ((line = reader.readLine()) != null) {

            if (linesToSkip > 0) {
                linesToSkip--;
                continue;
            }

            final Event event = JacksonEvent.fromMessage(line);
            eventConsumer.accept(new Record<>(event));
        }
    }
}
