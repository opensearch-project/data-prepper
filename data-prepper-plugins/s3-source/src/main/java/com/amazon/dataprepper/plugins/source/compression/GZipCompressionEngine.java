/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.compression;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class GZipCompressionEngine implements CompressionEngine {
    @Override
    public InputStream createInputStream(final String s3Key, final InputStream responseInputStream) throws IOException {
        return new GZIPInputStream(responseInputStream);
    }
}
