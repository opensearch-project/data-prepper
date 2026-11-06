/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnappyDecompressionEngineTest {

    private SnappyDecompressionEngine decompressionEngine;

    @BeforeEach
    void setUp() {
        decompressionEngine = new SnappyDecompressionEngine();
    }

    @Test
    @DisplayName("Pure Xerial framed, no header")
    void createInputStream_with_xerial_framed_no_header() throws IOException {
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = createXerialFramed(testString.getBytes(StandardCharsets.UTF_8));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(new String(result.readAllBytes(), StandardCharsets.UTF_8), equalTo(testString));
    }

    @Test
    @DisplayName("Pure raw Snappy, no header, large payload (>= 128 bytes original)")
    void createInputStream_with_raw_snappy_no_header_large_payload() throws IOException {
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("Pure raw Snappy, no header, tiny payload (< 32 bytes original)")
    void createInputStream_with_raw_snappy_no_header_tiny_payload() throws IOException {
        final byte[] original = "short".getBytes(StandardCharsets.UTF_8);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("Pure raw Snappy starting with 0x7B (123-byte original, first byte is '{')")
    void createInputStream_with_raw_snappy_starting_with_open_brace() throws IOException {
        final byte[] original = generateBytes(123);
        final byte[] compressed = Snappy.compress(original);
        // Verify first byte is indeed 0x7B
        assertThat(compressed[0], equalTo((byte) 0x7B));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("Pure raw Snappy starting with 0x5B (91-byte original, first byte is '[')")
    void createInputStream_with_raw_snappy_starting_with_open_bracket() throws IOException {
        final byte[] original = generateBytes(91);
        final byte[] compressed = Snappy.compress(original);
        // Verify first byte is indeed 0x5B
        assertThat(compressed[0], equalTo((byte) 0x5B));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header + Xerial framed payload")
    void createInputStream_with_json_header_and_xerial_framed() throws IOException {
        final String header = "{\"compression\": \"snappy\", \"topic\": \"test\"}\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = createXerialFramed(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(new String(result.readAllBytes(), StandardCharsets.UTF_8), equalTo(testString));
    }

    @Test
    @DisplayName("JSON header + raw Snappy payload")
    void createInputStream_with_json_header_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\", \"key\": \"value\"}\n";
        final byte[] original = generateBytes(512);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with CRLF delimiter + raw Snappy payload")
    void createInputStream_with_json_header_crlf_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\"}\r\n";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with multiple trailing newlines + raw Snappy payload")
    void createInputStream_with_json_header_multiple_newlines_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\"}\n\n";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with tab delimiter + raw Snappy payload")
    void createInputStream_with_json_header_tab_delimiter_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\"}\t";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header without any delimiter + raw Snappy payload")
    void createInputStream_with_json_header_no_delimiter_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\"}";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON array header + raw Snappy payload")
    void createInputStream_with_json_array_header_and_raw_snappy() throws IOException {
        final String header = "[{\"topic\": \"test\"}, {\"partition\": 0}]\n";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with UTF-8 values + raw Snappy payload")
    void createInputStream_with_utf8_json_header_and_raw_snappy() throws IOException {
        final String header = "{\"metadata\": \"données compressées\", \"key\": \"日本語\"}\n";
        final byte[] original = generateBytes(400);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with nested objects + raw Snappy payload")
    void createInputStream_with_nested_json_header_and_raw_snappy() throws IOException {
        final String header = "{\"meta\": {\"topic\": \"test\", \"nested\": {\"key\": \"val\"}}}\n";
        final byte[] original = generateBytes(300);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header with braces in string values does not confuse parser")
    void createInputStream_with_braces_in_json_values_and_raw_snappy() throws IOException {
        final String header = "{\"pattern\": \"{key}: {value}\", \"regex\": \"[a-z]\"}\n";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("Leading whitespace before JSON header + raw Snappy payload")
    void createInputStream_with_leading_whitespace_and_json_header() throws IOException {
        final String header = "  {\"compression\": \"snappy\"}\n";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("Xerial framed with large payload streams correctly")
    void createInputStream_with_xerial_framed_large_payload() throws IOException {
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = createXerialFramed(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("JSON header + Xerial framed with large payload streams correctly")
    void createInputStream_with_header_and_xerial_framed_large_payload() throws IOException {
        final String header = "{\"topic\": \"events\", \"partition\": 0}\n";
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = createXerialFramed(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    @DisplayName("No valid Snappy data throws IOException")
    void createInputStream_with_no_valid_snappy_throws_exception() {
        final byte[] plainText = "This is just plain text with no compression at all and more text to fill buffer"
                .getBytes(StandardCharsets.UTF_8);

        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(plainText)));
    }

    @Test
    @DisplayName("Empty stream throws IOException")
    void createInputStream_with_empty_stream_throws_exception() {
        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    @DisplayName("detectJsonHeaderEnd returns 0 when buffer does not start with '{' or '['")
    void detectJsonHeaderEnd_returns_zero_for_non_json_start() {
        final byte[] buffer = "not json".getBytes(StandardCharsets.UTF_8);
        assertThat(SnappyDecompressionEngine.detectJsonHeaderEnd(buffer), equalTo(0));
    }

    @Test
    @DisplayName("detectJsonHeaderEnd returns 0 when '{' is followed by invalid JSON")
    void detectJsonHeaderEnd_returns_zero_for_invalid_json() {
        final byte[] buffer = new byte[]{'{', (byte) 0xFF, (byte) 0xFE, 0x00, 0x01};
        assertThat(SnappyDecompressionEngine.detectJsonHeaderEnd(buffer), equalTo(0));
    }

    @Test
    @DisplayName("detectJsonHeaderEnd returns correct offset after JSON with newline")
    void detectJsonHeaderEnd_returns_offset_after_json_and_newline() {
        final byte[] buffer = "{\"key\": \"value\"}\nPAYLOAD".getBytes(StandardCharsets.UTF_8);
        final int offset = SnappyDecompressionEngine.detectJsonHeaderEnd(buffer);
        assertThat(new String(buffer, offset, buffer.length - offset, StandardCharsets.UTF_8), equalTo("PAYLOAD"));
    }

    @Test
    @DisplayName("detectJsonHeaderEnd returns correct offset after JSON without delimiter")
    void detectJsonHeaderEnd_returns_offset_after_json_no_delimiter() {
        final byte[] buffer = "{\"key\": \"value\"}PAYLOAD".getBytes(StandardCharsets.UTF_8);
        final int offset = SnappyDecompressionEngine.detectJsonHeaderEnd(buffer);
        assertThat(new String(buffer, offset, buffer.length - offset, StandardCharsets.UTF_8), equalTo("PAYLOAD"));
    }

    private byte[] createXerialFramed(final byte[] data) throws IOException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final SnappyOutputStream snappyOut = new SnappyOutputStream(byteOut);
        snappyOut.write(data);
        snappyOut.close();
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
