/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;

public class SnappyDecompressionEngine implements DecompressionEngine {

    @Override
    public InputStream createInputStream(final InputStream responseInputStream) throws IOException {
        return new SnappyInputStream(responseInputStream);
    }
}
