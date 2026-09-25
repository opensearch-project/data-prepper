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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests format detection against plain-text sample files from test resources.
 *
 * Compressed variants are generated at test time (rather than checked in) to keep
 * the repository free of binary artifacts.
 */
class FormatDetectorFileTest {

    private FormatDetector detector;

    @BeforeEach
    void setUp() {
        detector = new FormatDetector();
    }

    @ParameterizedTest(name = "{0} -> format={1}")
    @MethodSource("sampleFiles")
    void detectsFormatFromSampleFile(final String fileName,
                                     final DetectedFormat expectedFormat) throws IOException {
        final byte[] sample = readResource("samples/" + fileName);
        final FormatDetectionResult result = detector.detect(sample);

        assertThat(result.getFormat())
                .as("format for %s", fileName)
                .isEqualTo(expectedFormat);
        assertThat(result.getCompression()).isEqualTo(DetectedCompression.NONE);
    }

    static Stream<Arguments> sampleFiles() {
        return Stream.of(
                Arguments.of("sample.json", DetectedFormat.JSON),
                Arguments.of("sample.ndjson", DetectedFormat.NDJSON),
                Arguments.of("sample.csv", DetectedFormat.CSV),
                Arguments.of("sample.tsv", DetectedFormat.TSV),
                Arguments.of("sample.xml", DetectedFormat.XML),
                Arguments.of("sample.log", DetectedFormat.TEXT)
        );
    }

    @Test
    void detectsGzippedNdjsonGeneratedAtTestTime() throws IOException {
        final byte[] plain = readResource("samples/sample.ndjson");
        final byte[] gzipped = gzip(plain);

        final FormatDetectionResult result = detector.detect(gzipped);
        assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
        assertThat(result.getFormat()).isEqualTo(DetectedFormat.NDJSON);
    }

    @Test
    void detectsGzippedCsvGeneratedAtTestTime() throws IOException {
        final byte[] plain = readResource("samples/sample.csv");
        final byte[] gzipped = gzip(plain);

        final FormatDetectionResult result = detector.detect(gzipped);
        assertThat(result.getCompression()).isEqualTo(DetectedCompression.GZIP);
        assertThat(result.getFormat()).isEqualTo(DetectedFormat.CSV);
    }

    private byte[] readResource(final String path) throws IOException {
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IOException("Resource not found: " + path);
            }
            return is.readAllBytes();
        }
    }

    private byte[] gzip(final byte[] data) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (final GZIPOutputStream gos = new GZIPOutputStream(bos)) {
            gos.write(data);
        }
        return bos.toByteArray();
    }
}
