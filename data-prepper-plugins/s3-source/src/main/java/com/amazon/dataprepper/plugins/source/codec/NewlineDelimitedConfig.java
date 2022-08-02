/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedConfig {
    private final int skipLines = 0;

    @JsonProperty("header_destination")
    private String headerDestination;

    /**
     * The number of lines to skip from the start of the S3 object.
     * Use 0 to skip no lines.
     *
     * @return The number of lines to skip.
     */
    public int getSkipLines() {
        return skipLines;
    }

    /**
     * The key containing the header line of the S3 object.
     * If this option is specified then each Event will contain a header_destination field.
     *
     * @return The name of the header_destination field.
     */
    public String getHeaderDestination() {
        return headerDestination;
    }
}