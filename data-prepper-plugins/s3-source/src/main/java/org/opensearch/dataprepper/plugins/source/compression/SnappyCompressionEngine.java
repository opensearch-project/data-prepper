/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.compression;

import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;

public class SnappyCompressionEngine implements CompressionEngine {

    @Override
    public InputStream createInputStream(final String s3Key, final InputStream responseInputStream) throws IOException {
        return new SnappyInputStream(responseInputStream);
    }
}
