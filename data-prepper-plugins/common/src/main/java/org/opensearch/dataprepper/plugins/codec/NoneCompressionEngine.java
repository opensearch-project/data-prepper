/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.CompressionEngine;

import java.io.OutputStream;

public class NoneCompressionEngine implements CompressionEngine {
    @Override
    public OutputStream createOutputStream(final OutputStream outputStream) {
        return outputStream;
    }
}
