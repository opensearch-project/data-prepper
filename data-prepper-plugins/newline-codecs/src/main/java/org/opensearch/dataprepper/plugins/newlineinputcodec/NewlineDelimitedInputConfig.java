/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.newlineinputcodec;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.Objects;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedInputConfig {
    private int skipLines = 0;

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

    @AssertTrue(message = "header_destination must be either null or length greater than 0. It cannot be empty. " +
            "To make it null delete header_destination in your configuration YAML file")
    boolean isValidHeaderDestination() {
        return Objects.isNull(headerDestination) || (headerDestination.length() > 0);
    }
}