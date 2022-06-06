/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * Generates records where each record is on a single line.
 */
public class NewlineDelimitedRecordsGenerator implements RecordsGenerator {
    private final Random random = new Random();
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:hh:mm:ss");

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) throws IOException {
        try (final PrintWriter printWriter = new PrintWriter(outputStream)) {
            for (int i = 0; i < numberOfRecords; i++) {
                writeLine(printWriter);
            }
        }
    }

    private void writeLine(final PrintWriter printWriter) {
        final String dateString = dateTimeFormatter.format(LocalDateTime.now());
        final String line = "127.0.0.1 - - [" + dateString + "] GET /my/endpoint HTTP/1.1 200 " + random.nextInt(3000) + " \"http://localhost\" \"Mozilla/5.0\"";
        printWriter.write(line + "\n");
    }
}
