/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Collections;
import java.util.Map;

public class EntityConfig {

    @JsonProperty("key_attributes")
    @NotEmpty
    private Map<String, String> keyAttributes = Collections.emptyMap();

    @JsonProperty("attributes")
    @NotNull
    private Map<String, String> attributes = Collections.emptyMap();

    public Map<String, String> getKeyAttributes() {
        return Collections.unmodifiableMap(keyAttributes);
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }
}
