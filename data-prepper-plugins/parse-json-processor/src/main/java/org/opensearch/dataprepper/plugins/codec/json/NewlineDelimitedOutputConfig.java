/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedOutputConfig {
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }
}