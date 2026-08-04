/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.IOException;
import java.io.InputStream;

public class GZipDecompressionEngine implements DecompressionEngine {

    private static final byte[] GZIP_MAGIC = {(byte) 0x1F, (byte) 0x8B};

    @Override
    public byte[] getMagicBytes() {
        return GZIP_MAGIC.clone();
    }

    @Override
    public InputStream createInputStream(final InputStream inputStream) throws IOException {
        final byte[] buffer = CompressionMagicDetector.bufferLookAhead(
                inputStream, CompressionMagicDetector.SCAN_BUFFER_SIZE);

        if (buffer.length < GZIP_MAGIC.length) {
            throw new IOException("No valid gzip data found: stream too short");
        }

        // Fast path: magic at offset 0
        if (CompressionMagicDetector.matchesAt(buffer, 0, GZIP_MAGIC)) {
            // We are using GzipCompressorInputStream here to decompress because GZIPInputStream doesn't decompress
            // concatenated .gz files — it stops after the first member and silently ignores the rest.
            return new GzipCompressorInputStream(
                    CompressionMagicDetector.reconstructStream(buffer, 0, buffer.length, inputStream), true);
        }

        // Detection path: scan buffer for magic
        final int offset = CompressionMagicDetector.findMagicOffset(buffer, buffer.length, GZIP_MAGIC);
        if (offset >= 0) {
            return new GzipCompressorInputStream(
                    CompressionMagicDetector.reconstructStream(buffer, offset, buffer.length, inputStream), true);
        }

        throw new IOException("No valid gzip data found");
    }
}
