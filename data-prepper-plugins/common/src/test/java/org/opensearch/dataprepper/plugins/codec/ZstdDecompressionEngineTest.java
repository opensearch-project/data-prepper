/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZstdDecompressionEngineTest {

    private DecompressionEngine decompressionEngine;

    @BeforeEach
    void setUp() {
        decompressionEngine = new ZstdDecompressionEngine();
    }

    // Scenario 1: Pure zstd, no header
    @Test
    void createInputStream_with_zstd_no_header() throws IOException {
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = Zstd.compress(testString.getBytes(StandardCharsets.UTF_8));

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 2: JSON header + zstd
    @Test
    void createInputStream_with_json_header_and_zstd() throws IOException {
        final String header = "{\"compression\": \"zstd\", \"topic\": \"test\"}\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = Zstd.compress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 3: Multi-line header + zstd
    @Test
    void createInputStream_with_multiline_header_and_zstd() throws IOException {
        final String header = "line1: metadata\nline2: more metadata\nline3: even more\n";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = Zstd.compress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 4: Header without newline + zstd
    @Test
    void createInputStream_with_header_no_newline_and_zstd() throws IOException {
        final String header = "{\"compression\": \"zstd\"}";
        final String testString = UUID.randomUUID().toString();
        final byte[] compressed = Zstd.compress(testString.getBytes(StandardCharsets.UTF_8));
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(testString.getBytes(StandardCharsets.UTF_8)));
    }

    // Scenario 5: No valid zstd data (misconfigured)
    @Test
    void createInputStream_with_no_valid_zstd_throws_exception() {
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
    void createInputStream_with_large_zstd_payload() throws IOException {
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = Zstd.compress(original);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(compressed));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    // Additional: Large payload with header
    @Test
    void createInputStream_with_header_and_large_zstd_payload() throws IOException {
        final String header = "{\"topic\": \"events\", \"partition\": 0}\n";
        final byte[] original = generateBytes(100_000);
        final byte[] compressed = Zstd.compress(original);
        final byte[] combined = concatenate(header.getBytes(StandardCharsets.UTF_8), compressed);

        final InputStream result = decompressionEngine.createInputStream(new ByteArrayInputStream(combined));

        assertThat(result.readAllBytes(), equalTo(original));
    }

    @Test
    void getMagicBytes_returns_zstd_magic() {
        final byte[] expected = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD};
        assertThat(decompressionEngine.getMagicBytes(), equalTo(expected));
    }

    // Additional: Stream too short
    @Test
    void createInputStream_with_stream_shorter_than_magic_throws_exception() {
        assertThrows(IOException.class, () ->
                decompressionEngine.createInputStream(new ByteArrayInputStream(new byte[]{0x28, 0x00})));
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
