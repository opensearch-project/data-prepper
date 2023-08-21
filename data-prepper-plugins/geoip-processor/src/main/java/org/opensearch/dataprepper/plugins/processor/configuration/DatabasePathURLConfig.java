/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class DatabasePathURLConfig {

    @JsonProperty("url")
    @NotNull
    String url;

    /**
     * Get the configured database path for local path or S3 or URL
     * @return String
     */
    public String getUrl() {
        return url;
    }
}
