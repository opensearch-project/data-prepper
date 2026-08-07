/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.codec;

import java.io.IOException;
import java.io.OutputStream;

public interface CompressionEngine {
    OutputStream createOutputStream(OutputStream outputStream) throws IOException;
    default byte[] compress(byte[] payload) throws IOException {
        throw new RuntimeException("Unsupported Operation");
    }
}
