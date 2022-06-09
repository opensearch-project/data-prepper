/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class CompressionEngine {
    private final CompressionOption compressionOption;

    public CompressionEngine(CompressionOption compressionOption) {
        this.compressionOption = compressionOption;
    }

    InputStream createInputStream(String s3Key, InputStream responseInputStream) throws IOException {
        if (compressionOption.equals(CompressionOption.NONE))
            return responseInputStream;
        else if (compressionOption.equals(CompressionOption.GZIP))
            return new GZIPInputStream(responseInputStream);
        else {
            if (s3Key.endsWith(".gz")) {
                return new GZIPInputStream(responseInputStream);
            }
            else {
                return responseInputStream;
            }
        }
    }
}
