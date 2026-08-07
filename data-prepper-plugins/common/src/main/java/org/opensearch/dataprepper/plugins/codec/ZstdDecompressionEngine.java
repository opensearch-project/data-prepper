/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.IOException;
import java.io.InputStream;

public class ZstdDecompressionEngine implements DecompressionEngine {

    private static final byte[] ZSTD_MAGIC = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD};

    @Override
    public InputStream createInputStream(final InputStream responseInputStream) throws IOException {
        final byte[] buffer = CompressionMagicDetector.bufferLookAhead(
                responseInputStream, CompressionMagicDetector.SCAN_BUFFER_SIZE);

        if (buffer.length < ZSTD_MAGIC.length) {
            throw new IOException("No valid zstd data found: stream too short");
        }

        // Fast path: magic at offset 0
        if (CompressionMagicDetector.matchesAt(buffer, 0, ZSTD_MAGIC)) {
            return new ZstdCompressorInputStream(
                    CompressionMagicDetector.reconstructStream(buffer, 0, buffer.length, responseInputStream));
        }

        // Detection path: scan buffer for magic
        final int offset = CompressionMagicDetector.findMagicOffset(buffer, buffer.length, ZSTD_MAGIC);
        if (offset >= 0) {
            return new ZstdCompressorInputStream(
                    CompressionMagicDetector.reconstructStream(buffer, offset, buffer.length, responseInputStream));
        }

        throw new IOException("No valid zstd data found");
    }
}
