/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.opensearch.dataprepper.model.codec.CompressionEngine;

import java.io.IOException;
import java.io.OutputStream;

public class GZipCompressionEngine implements CompressionEngine {
    @Override
    public OutputStream createOutputStream(final OutputStream outputStream) throws IOException {
        return new GzipCompressorOutputStream(outputStream);
    }
}
