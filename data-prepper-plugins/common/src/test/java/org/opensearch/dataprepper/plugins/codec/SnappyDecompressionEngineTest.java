/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.junit.jupiter.api.BeforeEach;
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

    // Scenario 1: Pure Xerial framed, no header
    @Test
    void createInputStream_with_xerial_framed_no_header() throws IOException {
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = createXerialFramed(testString.getBytes(StandardCharsets.UTF_8));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(new String(result.readAllBytes(), StandardCharsets.UTF_8), equalTo(testString));
    }

    // Scenario 2: Pure raw Snappy, no header, large payload (>= 128 bytes original)
    @Test
    void createInputStream_with_raw_snappy_no_header_large_payload() throws IOException {
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 3: Pure raw Snappy, no header, tiny payload (< 32 bytes original)
    @Test
    void createInputStream_with_raw_snappy_no_header_tiny_payload() throws IOException {
        final byte[] original = "short".getBytes(StandardCharsets.UTF_8);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 4: JSON header + Xerial framed (large file)
    @Test
    void createInputStream_with_json_header_and_xerial_framed() throws IOException {
        final String header = "{\"compression\": \"snappy\", \"topic\": \"test\"}\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = createXerialFramed(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(new String(result.readAllBytes(), StandardCharsets.UTF_8), equalTo(testString));
    }

    // Scenario 5: JSON header + raw Snappy (large file)
    @Test
    void createInputStream_with_json_header_and_raw_snappy_large() throws IOException {
        final String header = "{\"compression\": \"snappy\", \"key\": \"value\"}\n";
        final byte[] original = generateBytes(512);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 6: JSON header WITHOUT newline + raw Snappy
    @Test
    void createInputStream_with_json_header_no_newline_and_raw_snappy() throws IOException {
        final String header = "{\"compression\": \"snappy\"}";
        final byte[] original = generateBytes(256);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 7: Multi-line header + raw Snappy
    @Test
    void createInputStream_with_multiline_header_and_raw_snappy() throws IOException {
        final String header = "line1: metadata\nline2: more metadata\nline3: even more\n";
        final byte[] original = generateBytes(300);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 8: Pure raw Snappy, 32-126B original, incompressible
    @Test
    void createInputStream_with_raw_snappy_ambiguous_range_incompressible() throws IOException {
        // Random bytes in 32-126 range are typically incompressible
        final byte[] original = generateBytes(64);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 9: Pure raw Snappy, 32-126B original, compressible
    @Test
    void createInputStream_with_raw_snappy_ambiguous_range_compressible() throws IOException {
        // Repeated pattern is compressible
        final byte[] original = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes(StandardCharsets.UTF_8);
        final byte[] compressed = Snappy.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 10: UTF-8 header + raw Snappy (large file)
    @Test
    void createInputStream_with_utf8_header_and_raw_snappy() throws IOException {
        final String header = "metadata: données compressées\n";
        final byte[] original = generateBytes(400);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Scenario 11: No valid Snappy at all (misconfigured)
    @Test
    void createInputStream_with_no_valid_snappy_throws_exception() {
        final byte[] plainText = "This is just plain text with no compression at all and more text to fill buffer"
                .getBytes(StandardCharsets.UTF_8);

        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(plainText)));
    }

    // Scenario 12: Empty stream
    @Test
    void createInputStream_with_empty_stream_throws_exception() {
        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(new byte[0])));
    }

    // Additional: Header + raw Snappy exceeding 4KB scan buffer
    @Test
    void createInputStream_with_header_and_raw_snappy_exceeding_scan_buffer() throws IOException {
        final String header = "{\"compression\": \"snappy\"}\n";
        final byte[] original = generateBytes(50_000);
        final byte[] compressed = Snappy.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Additional: Xerial framed with large payload (streaming verification)
    @Test
    void createInputStream_with_xerial_framed_large_payload() throws IOException {
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = createXerialFramed(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Additional: Header + Xerial framed with large payload
    @Test
    void createInputStream_with_header_and_xerial_framed_large_payload() throws IOException {
        final String header = "{\"topic\": \"events\", \"partition\": 0}\n";
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = createXerialFramed(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    void getMagicBytes_returns_xerial_magic() {
        final byte[] expected = {(byte) 0x82, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00};
        assertThat(decompressionEngine.getMagicBytes(), equalTo(expected));
    }

    @Test
    void decodeVarint_decodes_single_byte() {
        final byte[] buffer = {0x05};
        assertThat(SnappyDecompressionEngine.decodeVarint(buffer, 0), equalTo(5L));
    }

    @Test
    void decodeVarint_decodes_multi_byte() {
        // 300 = 0b100101100 → varint: 0xAC 0x02
        final byte[] buffer = {(byte) 0xAC, 0x02};
        assertThat(SnappyDecompressionEngine.decodeVarint(buffer, 0), equalTo(300L));
    }

    @Test
    void decodeVarint_returns_negative_one_for_unterminated() {
        final byte[] buffer = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80};
        assertThat(SnappyDecompressionEngine.decodeVarint(buffer, 0), equalTo(-1L));
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
