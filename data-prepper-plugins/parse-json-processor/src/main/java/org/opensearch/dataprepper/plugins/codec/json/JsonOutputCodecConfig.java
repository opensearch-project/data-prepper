/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class JsonOutputCodecConfig {

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }
}
