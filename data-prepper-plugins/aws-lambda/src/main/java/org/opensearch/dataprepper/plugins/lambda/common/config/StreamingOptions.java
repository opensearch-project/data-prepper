/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamingOptions {
    @JsonProperty("enabled")
    private boolean enabled = false;
    
    @JsonProperty("response_handling")
    private ResponseHandling responseHandling = ResponseHandling.RECONSTRUCT_DOCUMENT;

    public boolean isEnabled() {
        return enabled;
    }

    public ResponseHandling getResponseHandling() {
        return responseHandling;
    }
}
