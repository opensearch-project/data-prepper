/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class FormatDetectorTest {

    private FormatDetector detector;

    @BeforeEach
    void setUp() {
        detector = new FormatDetector();
    }

    @Nested
    class CompressionDetection {

        @Test
        void detectsGzip() throws IOException {
            final byte[] gzipData = gzip("{\"a\":1}\n{\"b\":2}\n".getBytes(StandardCharsets.UTF_8));
            assertThat(detector.detectCompression(gzipData)).isEqualTo(DetectedCompression.GZIP);
        }

        @Test
        void returnsNoneForUncompressed() {
            final byte[] plainData = "hello world this is plain text content".getBytes(StandardCharsets.UTF_8);
            assertThat(detector.detectCompression(plainData)).isEqualTo(DetectedCompression.NONE);
        }
    }

    @Nested
    class JsonDetection {

        @Test
        void detectsSingleJsonObjectAsNdjson() {
            final byte[] json = "{\"name\": \"test\", \"value\": 42}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
            assertThat(result.getCompression()).isEqualTo(DetectedCompression.NONE);
        }

        @Test
        void detectsJsonArray() {
            final byte[] json = "[{\"a\":1},{\"a\":2},{\"a\":3}]".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void detectsTruncatedJsonArrayAsMediumConfidence() {
            final byte[] json = "[{\"a\":1},{\"b\":2},{\"c\":3}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.MEDIUM);
        }

        @Test
        void handlesJsonWithEscapedBrackets() {
            final byte[] json = "{\"msg\": \"value with \\\"nested\\\" and {braces}\"}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }
    }

    @Nested
    class NdjsonDetection {

        @Test
        void detectsNdjson() {
            final String ndjson = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n{\"d\":4}\n{\"e\":5}\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void detectsNdjsonMediumWhenSomeLinesInvalid() {
            final String ndjson = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n{\"d\":4}\nnot json\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.MEDIUM);
        }

        @Test
        void doesNotDetectNdjsonWhenTooFewJsonLines() {
            final String text = "{\"a\":1}\nnot json\nalso not\nnope\nstill no\n";
            final FormatDetectionResult result = detector.detect(text.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isNotEqualTo(DetectedFormat.NDJSON);
        }

        @Test
        void detectsNdjsonWithEmptyLines() {
            final String ndjson = "{\"a\":1}\n\n{\"b\":2}\n\n{\"c\":3}\n\n{\"d\":4}\n\n{\"e\":5}\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
        }
    }

    @Nested
    class XmlDetection {

        @Test
        void detectsXmlWithDeclaration() {
            final byte[] xml = "<?xml version=\"1.0\"?><root><item/></root>".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(xml);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.XML);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void detectsXmlWithRootTag() {
            final byte[] xml = "<catalog><product>data</product></catalog>".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(xml);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.XML);
        }
    }

    @Nested
    class CsvDetection {

        @Test
        void detectsCsv() {
            final String csv = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
            final FormatDetectionResult result = detector.detect(csv.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.CSV);
            assertThat(result.getConfidence()).isEqualTo(Confidence.MEDIUM);
        }

        @Test
        void detectsTsv() {
            final String tsv = "name\tage\tcity\nAlice\t30\tNYC\nBob\t25\tLA\nCharlie\t35\tChicago\n";
            final FormatDetectionResult result = detector.detect(tsv.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.TSV);
        }

        @Test
        void detectsCsvWithQuotedCommas() {
            final String csv = "name,address,city\n\"Smith, John\",\"123 Main St\",NYC\n"
                    + "\"Doe, Jane\",\"456 Oak Ave\",LA\n\"Lee, Amy\",\"789 Pine Rd\",SF\n";
            final FormatDetectionResult result = detector.detect(csv.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isIn(DetectedFormat.CSV, DetectedFormat.TEXT);
        }

        @Test
        void doesNotDetectCsvForInconsistentDelimiters() {
            final String text = "one,two,three\nfour\nfive,six\n";
            final FormatDetectionResult result = detector.detect(text.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isNotEqualTo(DetectedFormat.CSV);
        }
    }

    @Nested
    class GzipIntegration {

        @Test
        void detectsGzippedNdjson() throws IOException {
            final String ndjson = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n{\"d\":4}\n{\"e\":5}\n";
            final byte[] gzipped = gzip(ndjson.getBytes(StandardCharsets.UTF_8));
            final FormatDetectionResult result = detector.detect(gzipped);
            assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
        }

        @Test
        void detectsGzippedCsv() throws IOException {
            final String csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
            final byte[] gzipped = gzip(csv.getBytes(StandardCharsets.UTF_8));
            final FormatDetectionResult result = detector.detect(gzipped);
            assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.CSV);
        }

        @Test
        void handlesCorruptGzip() {
            final byte[] corrupt = new byte[]{0x1F, (byte) 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F, 0x7F};
            final FormatDetectionResult result = detector.detect(corrupt);
            assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.UNKNOWN);
        }
    }

    @Nested
    class EdgeCases {

        @Test
        void returnsUnknownForNull() {
            final FormatDetectionResult result = detector.detect(null);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.UNKNOWN);
            assertThat(result.getConfidence()).isEqualTo(Confidence.LOW);
        }

        @Test
        void returnsUnknownForEmptyArray() {
            final FormatDetectionResult result = detector.detect(new byte[0]);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.UNKNOWN);
        }

        @Test
        void returnsUnknownForTinyInput() {
            final FormatDetectionResult result = detector.detect(new byte[]{0x00, 0x01});
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.UNKNOWN);
        }

        @Test
        void fallsBackToTextForPlainContent() {
            final String plain = "This is just a plain text log line.\nAnother line here.\nAnd another.\n";
            final FormatDetectionResult result = detector.detect(plain.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.TEXT);
            assertThat(result.getConfidence()).isEqualTo(Confidence.LOW);
        }

        @Test
        void detectsBinaryGarbageAsUnknown() {
            final byte[] garbage = new byte[512];
            for (int i = 0; i < garbage.length; i++) {
                garbage[i] = (byte) ((i * 37 + 13) % 256);
            }
            // Ensure it doesn't start with a known magic byte
            garbage[0] = 0x03;
            garbage[1] = 0x04;
            garbage[2] = 0x05;
            garbage[3] = 0x06;
            final FormatDetectionResult result = detector.detect(garbage);
            assertThat(result.getFormat()).isIn(DetectedFormat.UNKNOWN, DetectedFormat.IMAGE);
        }

        @Test
        void stripsBomBeforeDetection() {
            final byte[] bomJson = ("\uFEFF{\"event\":\"login\"}\n{\"event\":\"logout\"}\n")
                    .getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(bomJson);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
        }
    }

    // Helper
    private byte[] gzip(final byte[] data) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (final GZIPOutputStream gos = new GZIPOutputStream(bos)) {
            gos.write(data);
        }
        return bos.toByteArray();
    }
}
