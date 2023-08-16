/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

import java.io.IOException;
import java.io.InputStream;

public class NoneCompressionScenario implements CompressionScenario {
    @Override
    public CompressionOption getCompressionOption() {
        return CompressionOption.NONE;
    }

    @Override
    public InputStream decompressingInputStream(final InputStream inputStream) throws IOException {
        return inputStream;
    }
}
