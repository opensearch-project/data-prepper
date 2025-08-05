/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class EntityConfig {
    @JsonProperty("attributes")
    private Map<String, String> attributes;

    @JsonProperty("key_attributes")
    private Map<String, String> keyAttributes;

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> getKeyAttributes() {
        return keyAttributes;
    }
}