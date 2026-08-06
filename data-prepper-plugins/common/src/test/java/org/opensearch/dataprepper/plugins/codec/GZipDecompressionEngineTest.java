/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GZipDecompressionEngineTest {

    private DecompressionEngine decompressionEngine;

    @BeforeEach
    void setUp() {
        decompressionEngine = new GZipDecompressionEngine();
    }

    // Scenario 1: Pure gzip, no header
    @Test
    void createInputStream_with_gzip_no_header() throws IOException {
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = gzipCompress(testString.getBytes(StandardCharsets.UTF_8));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 2: JSON header + gzip
    @Test
    void createInputStream_with_json_header_and_gzip() throws IOException {
        final String header = "{\"compression\": \"gzip\", \"topic\": \"test\"}\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = gzipCompress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 3: Multi-line header + gzip
    @Test
    void createInputStream_with_multiline_header_and_gzip() throws IOException {
        final String header = "line1: metadata\nline2: more metadata\nline3: even more\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = gzipCompress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 4: Header without newline + gzip
    @Test
    void createInputStream_with_header_no_newline_and_gzip() throws IOException {
        final String header = "{\"compression\": \"gzip\"}";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = gzipCompress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 5: No valid gzip data (misconfigured)
    @Test
    void createInputStream_with_no_valid_gzip_throws_exception() {
        final byte[] plainText = "This is just plain text with no compression".getBytes(StandardCharsets.UTF_8);

        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(plainText)));
    }

    // Scenario 6: Empty stream
    @Test
    void createInputStream_with_empty_stream_throws_exception() {
        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(new byte[0])));
    }

    // Additional: Large payload with no header
    @Test
    void createInputStream_with_large_gzip_payload() throws IOException {
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = gzipCompress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Additional: Large payload with header
    @Test
    void createInputStream_with_header_and_large_gzip_payload() throws IOException {
        final String header = "{\"topic\": \"events\", \"partition\": 0}\n";
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = gzipCompress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Additional: Stream too short (1 byte)
    @Test
    void createInputStream_with_single_byte_throws_exception() {
        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(new byte[]{0x1F})));
    }

    private byte[] gzipCompress(final byte[] data) throws IOException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
        gzipOut.write(data);
        gzipOut.close();
        return byteOut.toByteArray();
    }

    private byte[] generateBytes(final int size) {
        final byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    private byte[] concatenate(final byte[] a, final byte[] b) {
        final byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
