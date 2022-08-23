/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import com.amazon.dataprepper.plugins.source.codec.CsvCodec;
import com.amazon.dataprepper.plugins.source.codec.CsvCodecConfig;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static com.amazon.dataprepper.test.helper.ReflectivelySetField.setField;

/**
 * Generates comma-separated CSV records where each record is on a single line.
 */
class CsvRecordsGenerator implements RecordsGenerator {
    private final String KNOWN_CLOUDFRONT_ACCESS_SNIPPET = "2022-06-30,15:03:52,DEL26,4628,88.217.18.190,PATCH," +
            "00b04893ad3c.cloudfront.net,app,200,erickson.info,Mozilla/5.0 (iPad; CPU iPad OS 14_2 like Mac OS X) AppleWebKit/533.0 " +
            "(KHTML, like Gecko) CriOS/15.0.895.0 Mobile/73X080 Safari/533.0";
    private final Random random = new Random();
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) {
        try (final PrintWriter printWriter = new PrintWriter(outputStream)) {
            for (int i = 0; i < numberOfRecords; i++) {
                writeLine(printWriter);
            }
        }
    }

    @Override
    public Codec getCodec() {
        CsvCodecConfig config = csvCodecConfigWithAutogenerateHeader();
        return new CsvCodec(config);
    }

    /**
     * For easy testing, we will autogenerate all column names (which requires setting detectHeader = false)
     * @return CsvCodecConfig for testing
     */
    private CsvCodecConfig csvCodecConfigWithAutogenerateHeader() {
        CsvCodecConfig csvCodecConfig = new CsvCodecConfig();
        try {
            setField(CsvCodecConfig.class, csvCodecConfig, "detectHeader", false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return csvCodecConfig;
    }

    @Override
    public String getFileExtension() {
        return "csv";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {
        final String message = event.get("message", String.class);
        assertThat(eventHasKnownLogSnippet(event, KNOWN_CLOUDFRONT_ACCESS_SNIPPET), equalTo(true));
    }

    private boolean eventHasKnownLogSnippet(final Event event, final String knownLogSnippet) {
        final String[] logSplitOnComma = knownLogSnippet.split(",");
        for (int columnIndex = 0; columnIndex < logSplitOnComma.length; columnIndex++) {
            final String field = logSplitOnComma[columnIndex];
            final String expectedColumnName = "column" + (columnIndex+1);
            if (!event.containsKey(expectedColumnName)) {
                return false;
            }
            if (!event.get(expectedColumnName, String.class).equals(field)) {
                return false;
            }
        }
        return true;
    }

    private void writeLine(final PrintWriter printWriter) {
        final String line = KNOWN_CLOUDFRONT_ACCESS_SNIPPET + "," + random.nextInt(3000);
        printWriter.write(line + "\n");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
