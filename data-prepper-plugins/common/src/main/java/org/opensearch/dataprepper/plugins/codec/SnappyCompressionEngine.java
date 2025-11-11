/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.CompressionEngine;
import org.xerial.snappy.SnappyOutputStream;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.OutputStream;

public class SnappyCompressionEngine implements CompressionEngine {
    @Override
    public OutputStream createOutputStream(final OutputStream outputStream) throws IOException {
        return new SnappyOutputStream(outputStream);
    }

    @Override
    public byte[] compress(byte[] payload) throws IOException {
        return Snappy.compress(payload);
    }
}
