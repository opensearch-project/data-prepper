/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.io.output.CountingOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class TestGenerator {
    private final Random random = new Random();

    public byte[] createJson(final int roughMaximumSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(roughMaximumSize);
        writeLog(roughMaximumSize, byteArrayOutputStream);
        return byteArrayOutputStream.toByteArray();
    }

    private void writeLog(final int roughMaximumSize, final OutputStream fileOutputStream) throws IOException {
        try (final CountingOutputStream countingOutputStream = new CountingOutputStream(fileOutputStream)) {
            writeJson(roughMaximumSize, countingOutputStream);
        }
    }

    private void writeJson(final int roughMaximumSize, final CountingOutputStream countingOutputStream) throws IOException {
        final JsonFactory jsonFactory = new JsonFactory();
        final JsonGenerator jsonGenerator = jsonFactory
                .createGenerator(countingOutputStream, JsonEncoding.UTF8);

        jsonGenerator.writeStartArray();

        while (countingOutputStream.getCount() < roughMaximumSize) {
            writeSingleRecord(jsonGenerator);
            jsonGenerator.flush(); // Need to flush the JsonGenerator in order to get the bytes to write to the counting output stream
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.close();

        countingOutputStream.flush();
    }

    private void writeSingleRecord(final JsonGenerator jsonGenerator) throws IOException {
        final StringBuilder logStringBuilder = new StringBuilder();
        logStringBuilder.append(Instant.now());
        logStringBuilder.append(" ");
        logStringBuilder.append(UUID.randomUUID());
        logStringBuilder.append(" ");
        logStringBuilder.append(UUID.randomUUID());
        logStringBuilder.append(" ");
        logStringBuilder.append(random.nextInt(100_000));

        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField("log", logStringBuilder.toString());

        jsonGenerator.writeEndObject();
    }
}
