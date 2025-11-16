/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamingOptions {
    @JsonProperty("enabled")
    private boolean enabled = false;
    
    @JsonProperty("response_handling")
    private String responseHandling = "reconstruct_document";

    public boolean isEnabled() {
        return enabled;
    }

    public String getResponseHandling() {
        return responseHandling;
    }
}
