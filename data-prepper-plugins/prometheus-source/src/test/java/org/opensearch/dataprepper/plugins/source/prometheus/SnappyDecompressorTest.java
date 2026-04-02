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

import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnappyDecompressorTest {

    @Test
    void testDecompressValidData() throws IOException {
        final byte[] original = "test data for snappy compression".getBytes();
        final byte[] compressed = Snappy.compress(original);

        final byte[] decompressed = SnappyDecompressor.decompress(compressed);

        assertThat(decompressed, equalTo(original));
    }

    @Test
    void testDecompressInvalidDataThrowsIOException() {
        final byte[] invalidData = "not snappy compressed".getBytes();

        assertThrows(IOException.class, () -> SnappyDecompressor.decompress(invalidData));
    }

    @Test
    void testDecompressEmptyData() {
        final byte[] emptyData = new byte[0];

        assertThrows(IOException.class, () -> SnappyDecompressor.decompress(emptyData));
    }
}