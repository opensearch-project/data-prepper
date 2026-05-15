/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.HashMap;
import java.util.Map;

public class EntityConfig {

    @JsonProperty("key_attributes")
    @NotEmpty
    private Map<String, String> keyAttributes;

    @JsonProperty("attributes")
    private Map<String, String> attributes = new HashMap<>();

    public Map<String, String> getKeyAttributes() {
        return keyAttributes;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
