/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class CompressionMagicDetectorTest {

    @Test
    void findMagicOffset_returns_zero_when_magic_at_start() {
        final byte[] magic = {0x1F, (byte) 0x8B};
        final byte[] buffer = {0x1F, (byte) 0x8B, 0x08, 0x00};

        assertThat(CompressionMagicDetector.findMagicOffset(buffer, buffer.length, magic), equalTo(0));
    }

    @Test
    void findMagicOffset_returns_offset_when_magic_after_header() {
        final byte[] magic = {0x1F, (byte) 0x8B};
        final byte[] buffer = new byte[]{'{', '"', 'a', '"', '}', '\n', 0x1F, (byte) 0x8B, 0x08};

        assertThat(CompressionMagicDetector.findMagicOffset(buffer, buffer.length, magic), equalTo(6));
    }

    @Test
    void findMagicOffset_returns_negative_one_when_magic_not_found() {
        final byte[] magic = {0x1F, (byte) 0x8B};
        final byte[] buffer = "plain text content".getBytes();

        assertThat(CompressionMagicDetector.findMagicOffset(buffer, buffer.length, magic), equalTo(-1));
    }

    @Test
    void findMagicOffset_returns_negative_one_for_empty_magic() {
        final byte[] buffer = {0x1F, (byte) 0x8B};

        assertThat(CompressionMagicDetector.findMagicOffset(buffer, buffer.length, new byte[0]), equalTo(-1));
    }

    @Test
    void findMagicOffset_returns_negative_one_when_buffer_shorter_than_magic() {
        final byte[] magic = {0x1F, (byte) 0x8B, 0x08, 0x00};
        final byte[] buffer = {0x1F, (byte) 0x8B};

        assertThat(CompressionMagicDetector.findMagicOffset(buffer, buffer.length, magic), equalTo(-1));
    }

    @Test
    void findMagicOffset_respects_bufferLength_parameter() {
        final byte[] magic = {0x1F, (byte) 0x8B};
        final byte[] buffer = new byte[]{'a', 'b', 0x1F, (byte) 0x8B};

        // bufferLength=2 means only first 2 bytes are considered
        assertThat(CompressionMagicDetector.findMagicOffset(buffer, 2, magic), equalTo(-1));
    }

    @Test
    void bufferLookAhead_reads_up_to_limit() throws IOException {
        final byte[] data = "hello world this is test data".getBytes();
        final InputStream stream = new ByteArrayInputStream(data);

        final byte[] result = CompressionMagicDetector.bufferLookAhead(stream, 5);

        assertThat(result.length, equalTo(5));
        assertThat(new String(result), equalTo("hello"));
    }

    @Test
    void bufferLookAhead_reads_all_when_stream_shorter_than_limit() throws IOException {
        final byte[] data = "hi".getBytes();
        final InputStream stream = new ByteArrayInputStream(data);

        final byte[] result = CompressionMagicDetector.bufferLookAhead(stream, 4096);

        assertThat(result.length, equalTo(2));
        assertThat(new String(result), equalTo("hi"));
    }

    @Test
    void reconstructStream_returns_stream_from_offset() throws IOException {
        final byte[] buffer = "header\npayload_start".getBytes();
        final byte[] remaining = "_end".getBytes();
        final InputStream remainingStream = new ByteArrayInputStream(remaining);

        final InputStream reconstructed = CompressionMagicDetector.reconstructStream(
                buffer, 7, buffer.length, remainingStream);

        final String result = new String(reconstructed.readAllBytes());
        assertThat(result, equalTo("payload_start_end"));
    }

    @Test
    void matchesAt_returns_true_for_matching_bytes() {
        final byte[] buffer = {0x00, 0x1F, (byte) 0x8B, 0x08};
        final byte[] magic = {0x1F, (byte) 0x8B};

        assertThat(CompressionMagicDetector.matchesAt(buffer, 1, magic), equalTo(true));
    }

    @Test
    void matchesAt_returns_false_for_non_matching_bytes() {
        final byte[] buffer = {0x00, 0x1F, 0x00, 0x08};
        final byte[] magic = {0x1F, (byte) 0x8B};

        assertThat(CompressionMagicDetector.matchesAt(buffer, 1, magic), equalTo(false));
    }

    @Test
    void matchesAt_returns_false_when_offset_too_close_to_end() {
        final byte[] buffer = {0x1F, (byte) 0x8B};
        final byte[] magic = {0x1F, (byte) 0x8B, 0x08};

        assertThat(CompressionMagicDetector.matchesAt(buffer, 0, magic), equalTo(false));
    }
}
