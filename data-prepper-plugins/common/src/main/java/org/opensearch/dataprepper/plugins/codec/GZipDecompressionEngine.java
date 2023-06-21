/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class GZipDecompressionEngine implements DecompressionEngine {
    @Override
    public InputStream createInputStream(final InputStream inputStream) throws IOException {
        final PushbackInputStream pushbackStream = new PushbackInputStream(inputStream, 2);
        final byte[] signature = new byte[2];
        //read the signature
        final int len = pushbackStream.read(signature);
        //push back the signature to the stream
        pushbackStream.unread(signature, 0, len);
        //check if matches standard gzip magic number
        if(!GzipCompressorInputStream.matches(signature, len)) {
            throw new IOException("GZIP encoding specified but data did contain gzip magic header");
        }

        // We are using GzipCompressorInputStream here to decompress because GZIPInputStream doesn't decompress concatenated .gz files
        // it stops after the first member and silently ignores the rest.
        // It doesn't leave the read position to point to the beginning of the next member.
        return new GzipCompressorInputStream(pushbackStream, true);
    }
}
