/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import java.io.IOException;
import java.io.OutputStream;

public interface CompressionEngine {
    OutputStream createOutputStream(OutputStream outputStream) throws IOException;
}
