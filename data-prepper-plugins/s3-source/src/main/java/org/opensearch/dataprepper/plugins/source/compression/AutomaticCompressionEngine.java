/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.compression;

import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;

import java.io.IOException;
import java.io.InputStream;

public class AutomaticCompressionEngine implements CompressionEngine {
    @Override
    public InputStream createInputStream(final String s3Key, final InputStream responseInputStream) throws IOException {
        return getCompressionOption(s3Key)
                .getEngine()
                .createInputStream(s3Key, responseInputStream);
    }

    private CompressionOption getCompressionOption(final String s3Key) {
        if (s3Key.endsWith(".gz")) {
            return CompressionOption.GZIP;
        } else {
            return CompressionOption.NONE;
        }

    }
}
