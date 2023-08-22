/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.InputStream;

public class NoneDecompressionEngine implements DecompressionEngine {
    @Override
    public InputStream createInputStream(final InputStream responseInputStream) {
        return responseInputStream;
    }
}
