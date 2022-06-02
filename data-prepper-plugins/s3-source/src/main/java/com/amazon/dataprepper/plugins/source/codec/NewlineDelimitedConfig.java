/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedConfig {
    private final int skipLines = 0;

    /**
     * The number of lines to skip from the start of the S3 object.
     * Use 0 to skip no lines.
     *
     * @return The number of lines to skip.
     */
    public int getSkipLines() {
        return skipLines;
    }
}
