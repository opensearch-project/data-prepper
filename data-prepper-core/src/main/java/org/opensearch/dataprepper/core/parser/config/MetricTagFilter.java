/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class MetricTagFilter {
    @JsonProperty("pattern")
    private String pattern;

    @JsonProperty("tags")
    private Map<String, String> tags = new LinkedHashMap<>();

    public MetricTagFilter() {
    }

    public MetricTagFilter(final String regex, final Map<String, String> tags) {
        this.pattern = regex;
        this.tags = tags;
    }

    public String getPattern() {
        return pattern;
    }

    public Map<String, String> getTags() {
        return Objects.requireNonNullElseGet(tags, HashMap::new);
    }
}
