/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;

public class SnappyCompressionScenario implements CompressionScenario {
    @Override
    public CompressionOption getCompressionOption() {
        return CompressionOption.SNAPPY;
    }

    @Override
    public InputStream decompressingInputStream(final InputStream inputStream) throws IOException {
        return new SnappyInputStream(inputStream);
    }

    @Override
    public String toString() {
        return "Snappy";
    }
}
