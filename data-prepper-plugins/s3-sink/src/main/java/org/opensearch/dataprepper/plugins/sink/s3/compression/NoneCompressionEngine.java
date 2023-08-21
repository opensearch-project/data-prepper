/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import java.io.OutputStream;

class NoneCompressionEngine implements CompressionEngine {
    @Override
    public OutputStream createOutputStream(final OutputStream outputStream) {
        return outputStream;
    }
}
