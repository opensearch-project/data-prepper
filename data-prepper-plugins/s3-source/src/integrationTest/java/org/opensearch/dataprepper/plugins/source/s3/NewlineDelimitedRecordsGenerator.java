/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.codec.newline.NewlineDelimitedInputCodec;
import org.opensearch.dataprepper.plugins.codec.newline.NewlineDelimitedInputConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Generates records where each record is on a single line.
 */
class NewlineDelimitedRecordsGenerator implements RecordsGenerator {
    private final String KNOWN_HTTP_LINE = "GET /my/endpoint HTTP/1.1 200";
    private final Random random = new Random();
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:hh:mm:ss");

    @Override
    public void write(final File file, int numberOfRecords) {
        try (final PrintWriter printWriter = new PrintWriter(file)) {
            for (int i = 0; i < numberOfRecords; i++) {
                writeLine(printWriter);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputCodec getCodec() {
        return new NewlineDelimitedInputCodec(new NewlineDelimitedInputConfig(), TestEventFactory.getTestEventFactory());
    }

    @Override
    public String getFileExtension() {
        return "txt";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {
        final String message = event.get("message", String.class);
        assertThat(message, notNullValue());
        assertThat(message, containsString(KNOWN_HTTP_LINE));
    }

    @Override
    public String getS3SelectExpression() {
        return null;
    }

    private void writeLine(final PrintWriter printWriter) {
        final String dateString = dateTimeFormatter.format(LocalDateTime.now());
        final String line = "127.0.0.1 - - [" + dateString + "] " + KNOWN_HTTP_LINE + " " + random.nextInt(3000) + " \"http://localhost\" \"Mozilla/5.0\"";
        printWriter.write(line + "\n");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean canCompress() {
        return true;
    }
}
