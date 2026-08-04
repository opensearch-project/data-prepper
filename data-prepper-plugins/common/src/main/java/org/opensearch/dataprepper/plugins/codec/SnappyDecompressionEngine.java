/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class SnappyDecompressionEngine implements DecompressionEngine {

    private static final byte[] XERIAL_MAGIC = {
            (byte) 0x82, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00
    };

    private static final int MAX_UNCOMPRESSED_SIZE = Integer.MAX_VALUE;

    @Override
    public byte[] getMagicBytes() {
        return XERIAL_MAGIC.clone();
    }

    @Override
    public InputStream createInputStream(final InputStream responseInputStream) throws IOException {
        final byte[] buffer = CompressionMagicDetector.bufferLookAhead(
                responseInputStream, CompressionMagicDetector.SCAN_BUFFER_SIZE);

        if (buffer.length == 0) {
            throw new IOException("Unable to detect valid Snappy-compressed data: stream is empty");
        }

        // Step 2: Fast path — check first bytes
        if (buffer.length >= XERIAL_MAGIC.length && CompressionMagicDetector.matchesAt(buffer, 0, XERIAL_MAGIC)) {
            // Xerial framed format at offset 0
            return new SnappyInputStream(CompressionMagicDetector.reconstructStream(
                    buffer, 0, buffer.length, responseInputStream));
        }

        final int firstByte = buffer[0] & 0xFF;

        if (firstByte >= 0x80) {
            // Non-ASCII first byte, not Xerial magic → raw Snappy with large payload
            return decompressRawSnappyFromStream(buffer, responseInputStream);
        }

        if (firstByte <= 0x1F) {
            // Control char range → raw Snappy with tiny payload
            return decompressRawSnappyFromStream(buffer, responseInputStream);
        }

        // Step 3: First byte is printable text (0x20-0x7E) — possible header or ambiguous raw Snappy

        // Step 3b: Check if entire buffer is valid raw Snappy at offset 0 (ambiguous range: 32-126 byte originals)
        final byte[] fullPayload = readFullPayload(buffer, responseInputStream);
        if (Snappy.isValidCompressedBuffer(fullPayload, 0, fullPayload.length)) {
            return new ByteArrayInputStream(Snappy.uncompress(fullPayload));
        }

        // Step 3c: Single-pass scan with two anchors
        final ScanResult result = scanForSnappy(fullPayload);
        if (result != null) {
            if (result.isXerial) {
                return new SnappyInputStream(new ByteArrayInputStream(
                        Arrays.copyOfRange(fullPayload, result.offset, fullPayload.length)));
            } else {
                return new ByteArrayInputStream(decompressRawSnappy(
                        fullPayload, result.offset, fullPayload.length - result.offset));
            }
        }

        // Step 3d: No valid Snappy found
        throw new IOException("Unable to detect valid Snappy-compressed data");
    }

    ScanResult scanForSnappy(final byte[] buffer) throws IOException {
        for (int i = 1; i < buffer.length; i++) {
            // Anchor 1: Xerial magic
            if (i + XERIAL_MAGIC.length <= buffer.length
                    && CompressionMagicDetector.matchesAt(buffer, i, XERIAL_MAGIC)) {
                return new ScanResult(i, true);
            }

            // Anchor 2: Varint + structural validation
            final long claimedLength = decodeVarint(buffer, i);
            if (claimedLength > 0 && claimedLength < MAX_UNCOMPRESSED_SIZE) {
                final int remaining = buffer.length - i;
                if (Snappy.isValidCompressedBuffer(buffer, i, remaining)) {
                    return new ScanResult(i, false);
                }
            }
        }
        return null;
    }

    /**
     * Decodes a Snappy varint (little-endian, base-128) at the given offset.
     * Returns the decoded value, or -1 if invalid.
     */
    static long decodeVarint(final byte[] buffer, final int offset) {
        long result = 0;
        int shift = 0;
        for (int i = offset; i < buffer.length && shift < 35; i++) {
            final byte b = buffer[i];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        return -1;
    }

    private byte[] decompressRawSnappy(final byte[] buffer, final int offset, final int length) throws IOException {
        if (!Snappy.isValidCompressedBuffer(buffer, offset, length)) {
            throw new IOException("Unable to detect valid Snappy-compressed data: invalid raw Snappy block");
        }
        final int uncompressedLength = Snappy.uncompressedLength(buffer, offset, length);
        final byte[] output = new byte[uncompressedLength];
        Snappy.uncompress(buffer, offset, length, output, 0);
        return output;
    }

    private InputStream decompressRawSnappyFromStream(final byte[] initialBuffer,
                                                      final InputStream remaining) throws IOException {
        final byte[] fullPayload = readFullPayload(initialBuffer, remaining);
        return new ByteArrayInputStream(decompressRawSnappy(fullPayload, 0, fullPayload.length));
    }

    private byte[] readFullPayload(final byte[] initialBuffer, final InputStream remaining) throws IOException {
        final byte[] rest = remaining.readAllBytes();
        if (rest.length == 0) {
            return initialBuffer;
        }
        final byte[] full = new byte[initialBuffer.length + rest.length];
        System.arraycopy(initialBuffer, 0, full, 0, initialBuffer.length);
        System.arraycopy(rest, 0, full, initialBuffer.length, rest.length);
        return full;
    }

    static class ScanResult {
        final int offset;
        final boolean isXerial;

        ScanResult(final int offset, final boolean isXerial) {
            this.offset = offset;
            this.isXerial = isXerial;
        }
    }
}
