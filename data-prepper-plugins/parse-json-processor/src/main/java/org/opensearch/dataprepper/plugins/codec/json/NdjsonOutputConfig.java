/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/**
 * Configuration class for the newline delimited codec.
 */
public class NdjsonOutputConfig {
    @JsonProperty("extension")
    @JsonPropertyDescription("defines the extension of the output file")
    private ExtensionOption extension = ExtensionOption.NDJSON;

    public ExtensionOption getExtensionOption() {
        return extension != null ? extension : ExtensionOption.NDJSON;
    }
}