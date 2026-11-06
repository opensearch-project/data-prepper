/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;

/**
 * Shared utility for detecting compression magic bytes in streams
 * that may have pre-compression text headers.
 */
final class CompressionMagicDetector {

    static final int SCAN_BUFFER_SIZE = 4096;

    private CompressionMagicDetector() {
    }

    /**
     * Scans a buffer for the given magic byte sequence.
     *
     * @return the offset where magic was found, or -1 if not found
     */
    static int findMagicOffset(final byte[] buffer, final int bufferLength, final byte[] magic) {
        if (magic.length == 0 || bufferLength < magic.length) {
            return -1;
        }
        final int limit = bufferLength - magic.length + 1;
        for (int i = 0; i < limit; i++) {
            if (matchesAt(buffer, i, magic)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Reads up to {@code limit} bytes from the stream into a buffer.
     */
    static byte[] bufferLookAhead(final InputStream stream, final int limit) throws IOException {
        return stream.readNBytes(limit);
    }

    /**
     * Reconstructs a stream starting from the given offset in the buffer,
     * followed by the remaining unconsumed bytes of the original stream.
     */
    static InputStream reconstructStream(final byte[] buffer, final int offset, final int bufferLength,
                                         final InputStream remaining) {
        final byte[] remainingBuffer = Arrays.copyOfRange(buffer, offset, bufferLength);
        return new SequenceInputStream(new ByteArrayInputStream(remainingBuffer), remaining);
    }

    static boolean matchesAt(final byte[] buffer, final int offset, final byte[] magic) {
        if (offset + magic.length > buffer.length) {
            return false;
        }
        for (int j = 0; j < magic.length; j++) {
            if (buffer[offset + j] != magic[j]) {
                return false;
            }
        }
        return true;
    }
}
