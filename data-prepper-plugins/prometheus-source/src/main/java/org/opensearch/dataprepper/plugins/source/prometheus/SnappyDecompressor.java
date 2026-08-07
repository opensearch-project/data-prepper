/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Handles Snappy decompression for Prometheus Remote Write payloads.
 */
public class SnappyDecompressor {

    private SnappyDecompressor() {
    }

    /**
     * Decompresses a Snappy-compressed byte array.
     *
     * @param compressed the Snappy-compressed data
     * @return the decompressed data
     * @throws IOException if decompression fails
     */
    public static byte[] decompress(final byte[] compressed) throws IOException {
        try {
            return Snappy.uncompress(compressed);
        } catch (final IOException e) {
            throw new IOException("Failed to decompress Snappy payload", e);
        }
    }
}