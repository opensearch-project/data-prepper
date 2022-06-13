/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.compression;

import java.io.InputStream;

public class NoneCompressionEngine implements CompressionEngine {
    @Override
    public InputStream createInputStream(final String s3Key, final InputStream responseInputStream) {
        return responseInputStream;
    }
}
