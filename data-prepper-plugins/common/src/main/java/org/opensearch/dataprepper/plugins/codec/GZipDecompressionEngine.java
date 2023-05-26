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
    @Override
    public InputStream createInputStream(final InputStream responseInputStream) throws IOException {
        // We are using GzipCompressorInputStream here to decompress because GZIPInputStream doesn't decompress concatenated .gz files
        // it stops after the first member and silently ignores the rest.
        // It doesn't leave the read position to point to the beginning of the next member.
        return new GzipCompressorInputStream(responseInputStream, true);
    }
}
