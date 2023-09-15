/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3SelectJsonOption {
    static final String DEFAULT_TYPE = "DOCUMENT";
    @JsonProperty("type")
    private String type = DEFAULT_TYPE;
    public String getType() {
        return type;
    }
}
