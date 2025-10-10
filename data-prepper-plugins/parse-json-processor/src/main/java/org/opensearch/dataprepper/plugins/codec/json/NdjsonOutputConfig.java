/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for the newline delimited codec.
 */
public class NdjsonOutputConfig {
    @JsonProperty("extension")
    private String extension;

    public String getExtension() {
        return extension;
    }
}