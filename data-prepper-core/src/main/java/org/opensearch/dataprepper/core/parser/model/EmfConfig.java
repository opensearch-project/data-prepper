/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class EmfConfig {
    @JsonProperty("additional_properties")
    private Map<String, String> additionalProperties = new HashMap<>();

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }
}
