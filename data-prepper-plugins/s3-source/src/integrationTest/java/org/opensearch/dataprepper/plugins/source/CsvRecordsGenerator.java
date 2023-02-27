/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.csvinputcodec.CsvCodecConfig;
import org.opensearch.dataprepper.plugins.csvinputcodec.CsvInputCodec;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

/**
 * Generates comma-separated CSV records where each record is on a single line.
 */
class CsvRecordsGenerator implements RecordsGenerator {
    private static final String KNOWN_CLOUDFRONT_ACCESS_SNIPPET = "2022-06-30,15:03:52,DEL26,4628,88.217.18.190,PATCH," +
            "00b04893ad3c.cloudfront.net,app,200,erickson.info,Mozilla/5.0 (iPad; CPU iPad OS 14_2 like Mac OS X) AppleWebKit/533.0 " +
            "(KHTML, like Gecko) CriOS/15.0.895.0 Mobile/73X080 Safari/533.0";
    private final Random random = new Random();

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) {
        try (final PrintWriter printWriter = new PrintWriter(outputStream)) {
            for (int i = 0; i < numberOfRecords; i++) {
                writeLine(printWriter);
            }
        }
    }

    @Override
    public InputCodec getCodec() {
        CsvCodecConfig config = csvCodecConfigWithAutogenerateHeader();
        return new CsvInputCodec(config);
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
