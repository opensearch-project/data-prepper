/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.Objects;

public interface InputCodec {
    /**
     * Parses an {@link InputStream}. Implementors should call the {@link Consumer} for each
     * {@link Record} loaded from the {@link InputStream}.
     *
     * @param inputStream   The input stream for code to process
     * @param eventConsumer The consumer which handles each event from the stream
     * @throws IOException throws IOException when invalid input is received or incorrect codec name is provided
     */
    void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException;

    /**
     * Parses an {@link InputFile}. Implementors should call the {@link Consumer} for each
     * {@link Record} loaded from the {@link InputFile}
     * @param inputFile The input file for the codec to process
     * @param decompressionEngine The engine to use to decompress the input file
     * @param eventConsumer The consumer which handles each event from the stream
     * @throws IOException throws IOException when invalid input is received or incorrect codec name is provided
     */
    default void parse(
            InputFile inputFile,
            DecompressionEngine decompressionEngine,
            Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputFile);
        Objects.requireNonNull(eventConsumer);
        System.out.println("======InputFile==="+inputFile);
        try (InputStream inputStream = inputFile.newStream()) {
            parse(decompressionEngine.createInputStream(inputStream), eventConsumer);
        }
    }
}
