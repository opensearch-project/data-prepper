/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.compression;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class AutomaticCompressionEngine implements CompressionEngine {
    @Override
    public InputStream createInputStream(final String s3Key, final InputStream responseInputStream) throws IOException {
        if (s3Key.endsWith(".gz")) {
            return new GZIPInputStream(responseInputStream);
        }
        else {
            return responseInputStream;
        }
    }
}
