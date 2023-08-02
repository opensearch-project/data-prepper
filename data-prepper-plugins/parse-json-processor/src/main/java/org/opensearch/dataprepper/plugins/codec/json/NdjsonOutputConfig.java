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
public class NdjsonOutputConfig {
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;


    @JsonProperty("include_keys")
    private List<String> includeKeys = DEFAULT_EXCLUDE_KEYS;

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public void setExcludeKeys(List<String> excludeKeys) {
        this.excludeKeys = excludeKeys;
    }

    public void setIncludeKeys(List<String> includeKeys) {
        this.includeKeys = includeKeys;
    }
}