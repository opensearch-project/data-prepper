/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests using real sample files from test resources.
 * Each file represents a realistic data sample that might be found in S3.
 */
class FormatDetectorFileTest {

    private FormatDetector detector;

    @BeforeEach
    void setUp() {
        detector = new FormatDetector();
    }

    @ParameterizedTest(name = "{0} → format={1}, compression={2}")
    @MethodSource("sampleFiles")
    void detectsFormatFromSampleFile(final String fileName,
                                     final DetectedFormat expectedFormat,
                                     final DetectedCompression expectedCompression,
                                     final Confidence minConfidence) throws IOException {
        final byte[] sample = readResource("samples/" + fileName);

        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getFormat())
                .as("format for %s", fileName)
                .isEqualTo(expectedFormat);
        assertThat(result.getCompression())
                .as("compression for %s", fileName)
                .isEqualTo(expectedCompression);
        assertThat(result.getConfidence().ordinal())
                .as("confidence for %s should be at least %s", fileName, minConfidence)
                .isLessThanOrEqualTo(minConfidence.ordinal());
    }

    static Stream<Arguments> sampleFiles() {
        return Stream.of(
                Arguments.of("sample.json", DetectedFormat.JSON, DetectedCompression.NONE, Confidence.HIGH),
                Arguments.of("sample.ndjson", DetectedFormat.NDJSON, DetectedCompression.NONE, Confidence.HIGH),
                Arguments.of("sample.csv", DetectedFormat.CSV, DetectedCompression.NONE, Confidence.MEDIUM),
                Arguments.of("sample.tsv", DetectedFormat.TSV, DetectedCompression.NONE, Confidence.MEDIUM),
                Arguments.of("sample.xml", DetectedFormat.XML, DetectedCompression.NONE, Confidence.HIGH),
                Arguments.of("sample.log", DetectedFormat.TEXT, DetectedCompression.NONE, Confidence.LOW),
                Arguments.of("sample.ndjson.gz", DetectedFormat.NDJSON, DetectedCompression.GZIP, Confidence.HIGH),
                Arguments.of("sample.parquet", DetectedFormat.PARQUET, DetectedCompression.NONE, Confidence.HIGH),
                Arguments.of("sample.avro", DetectedFormat.AVRO, DetectedCompression.NONE, Confidence.HIGH),
                Arguments.of("sample.orc", DetectedFormat.ORC, DetectedCompression.NONE, Confidence.HIGH)
        );
    }

    @Test
    void jsonFileDetectedWithHighConfidence() throws IOException {
        final byte[] sample = readResource("samples/sample.json");
        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getFormat()).isEqualTo(DetectedFormat.JSON);
        assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
        assertThat(result.getCompression()).isEqualTo(DetectedCompression.NONE);
    }

    @Test
    void gzippedNdjsonDetectsBothCompressionAndFormat() throws IOException {
        final byte[] sample = readResource("samples/sample.ndjson.gz");
        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
        assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
    }

    @Test
    void parquetDetectedByMagicBytes() throws IOException {
        final byte[] sample = readResource("samples/sample.parquet");
        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getFormat()).isEqualTo(DetectedFormat.PARQUET);
        assertThat(result.getConfidence()).isEqualTo(Confidence.HIGH);
    }

    @Test
    void plainLogFallsBackToText() throws IOException {
        final byte[] sample = readResource("samples/sample.log");
        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getFormat()).isEqualTo(DetectedFormat.TEXT);
        assertThat(result.getConfidence()).isEqualTo(Confidence.LOW);
    }

    private byte[] readResource(final String path) throws IOException {
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IOException("Resource not found: " + path);
            }
            return is.readAllBytes();
        }
    }
}
