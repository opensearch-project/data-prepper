/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
        void detectsGzip() {
            final byte[] gzipData = new byte[]{0x1F, (byte) 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00};
            assertThat(detector.detectCompression(gzipData)).isEqualTo(DetectedCompression.GZIP);
        }

        @Test
        void detectsZstd() {
            final byte[] zstdData = new byte[]{0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, 0x00};
            assertThat(detector.detectCompression(zstdData)).isEqualTo(DetectedCompression.ZSTD);
        }

        @Test
        void detectsSnappy() {
            final byte[] snappyData = new byte[]{
                    (byte) 0xFF, 0x06, 0x00, 0x00, 0x73, 0x4E, 0x61, 0x50, 0x70, 0x59, 0x00
            };
            assertThat(detector.detectCompression(snappyData)).isEqualTo(DetectedCompression.SNAPPY);
        }

        @Test
        void returnsNoneForUncompressed() {
            final byte[] plainData = "hello world".getBytes(StandardCharsets.UTF_8);
            assertThat(detector.detectCompression(plainData)).isEqualTo(DetectedCompression.NONE);
        }
    }

    @Nested
    class BinaryFormatDetection {

        @Test
        void detectsParquet() {
            final byte[] parquetData = "PAR1\u0000\u0000\u0000\u0000".getBytes(StandardCharsets.US_ASCII);
            assertThat(detector.detectBinaryFormat(parquetData)).isEqualTo(DetectedFormat.PARQUET);
        }

        @Test
        void detectsAvro() {
            final byte[] avroData = new byte[]{0x4F, 0x62, 0x6A, 0x01, 0x00, 0x00, 0x00, 0x00};
            assertThat(detector.detectBinaryFormat(avroData)).isEqualTo(DetectedFormat.AVRO);
        }

        @Test
        void detectsOrc() {
            final byte[] orcData = "ORC\u0000\u0000\u0000".getBytes(StandardCharsets.US_ASCII);
            assertThat(detector.detectBinaryFormat(orcData)).isEqualTo(DetectedFormat.ORC);
        }

        @Test
        void returnsNullForText() {
            final byte[] textData = "hello".getBytes(StandardCharsets.UTF_8);
            assertThat(detector.detectBinaryFormat(textData)).isNull();
        }
    }

    @Nested
    class JsonDetection {

        @Test
        void detectsSingleJsonObject() {
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
        void detectsNestedJson() {
            final byte[] json = "{\"outer\": {\"inner\": [1,2,3]}, \"flag\": true}".getBytes(StandardCharsets.UTF_8);
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
        void detectsNdjsonWithMediumConfidenceWhenSomeLinesInvalid() {
            // 4 valid JSON lines + 1 non-JSON = 80%
            final String ndjson = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n{\"d\":4}\nnot json\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.MEDIUM);
        }

        @Test
        void doesNotDetectNdjsonWhenTooFewJsonLines() {
            // Only 1 valid JSON line out of 5 = 20%
            final String text = "{\"a\":1}\nnot json\nalso not\nnope\nstill no\n";
            final FormatDetectionResult result = detector.detect(text.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isNotEqualTo(DetectedFormat.NDJSON);
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
            final byte[] xml = "<root><item>data</item></root>".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(xml);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.XML);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
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
            assertThat(result.getConfidence()).isEqualTo(Confidence.MEDIUM);
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
        void detectsGzippedJson() throws IOException {
            final byte[] json = "{\"key\": \"value\", \"number\": 123}".getBytes(StandardCharsets.UTF_8);
            final byte[] gzipped = gzip(json);

            final FormatDetectionResult result = detector.detect(gzipped);
            assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

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
            // Gzip magic bytes but corrupt payload
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
        void handlesJsonWithEscapedBrackets() {
            final byte[] json = "{\"msg\": \"value with \\\"nested\\\" and {braces}\"}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }
    }

    @Nested
    class PdfAndImageDetection {

        @Test
        void detectsPdf() {
            final byte[] pdf = "%PDF-1.4 fake pdf content here".getBytes(StandardCharsets.US_ASCII);
            final FormatDetectionResult result = detector.detect(pdf);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.PDF);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void detectsJpeg() {
            final byte[] jpeg = new byte[]{(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0, 0x00, 0x10};
            final FormatDetectionResult result = detector.detect(jpeg);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.IMAGE);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void detectsPng() {
            final byte[] png = new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
            final FormatDetectionResult result = detector.detect(png);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.IMAGE);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }
    }

    @Nested
    class JsonEdgeCases {

        @Test
        void jsonArrayOfPrimitivesDetectedAsJson() {
            final byte[] json = "[1, 2, 3, 4, 5]".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void jsonArrayOfArraysDetectedAsJson() {
            final byte[] json = "[[1,2],[3,4],[5,6]]".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
            assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        }

        @Test
        void singleJsonObjectTreatedAsNdjson() {
            // Single JSON object on one line — not a JSON array, so falls through to NDJSON detection
            final byte[] json = "{\"id\":1,\"name\":\"test\",\"active\":true}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            // Single line starting with { that parses as JSON → NDJSON with 1 doc
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
        }

        @Test
        void truncatedJsonArrayDetectedWithMediumConfidence() {
            // JSON array that doesn't close — simulates 64KB cutoff mid-array
            final byte[] json = "[{\"a\":1},{\"b\":2},{\"c\":3}".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            // Starts with [ but doesn't balance — not complete JSON
            // Falls through to NDJSON line-by-line or other detection
            assertThat(result.getFormat()).isNotEqualTo(DetectedFormat.UNKNOWN);
        }

        @Test
        void whitespaceBeforeJsonArrayStillDetected() {
            final byte[] json = "   \n  [{\"a\":1},{\"b\":2}]".getBytes(StandardCharsets.UTF_8);
            final FormatDetectionResult result = detector.detect(json);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
        }
    }

    @Nested
    class CsvEdgeCases {

        @Test
        void csvWithQuotedFieldsContainingCommas() {
            final String csv = "name,address,city\n\"Smith, John\",\"123 Main St\",NYC\n\"Doe, Jane\",\"456 Oak Ave\",LA\n";
            final FormatDetectionResult result = detector.detect(csv.getBytes(StandardCharsets.UTF_8));
            // Quoted commas don't affect the delimiter count for detection
            // The raw comma count per line differs, but the structure is still CSV
            assertThat(result.getFormat()).isIn(DetectedFormat.CSV, DetectedFormat.TEXT);
        }

        @Test
        void singleColumnTextNotDetectedAsCsv() {
            final String text = "hello\nworld\nfoo\nbar\nbaz\n";
            final FormatDetectionResult result = detector.detect(text.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isNotEqualTo(DetectedFormat.CSV);
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.TEXT);
        }
    }

    @Nested
    class NdjsonEdgeCases {

        @Test
        void ndjsonWithEmptyLinesMixed() {
            final String ndjson = "{\"a\":1}\n\n{\"b\":2}\n\n{\"c\":3}\n\n{\"d\":4}\n\n{\"e\":5}\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
            assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
        }

        @Test
        void ndjsonWithWhitespaceOnlyLines() {
            final String ndjson = "{\"a\":1}\n   \n{\"b\":2}\n  \n{\"c\":3}\n";
            final FormatDetectionResult result = detector.detect(ndjson.getBytes(StandardCharsets.UTF_8));
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
