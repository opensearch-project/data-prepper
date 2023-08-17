/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

import java.io.IOException;
import java.io.InputStream;

/**
 * A scenario for whole-file compression.
 */
public interface CompressionScenario {
    CompressionOption getCompressionOption();
    InputStream decompressingInputStream(final InputStream inputStream) throws IOException;
}
