/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public class MetricTagFilter {
    @JsonProperty("regex")
    private String regex;

    @JsonProperty("tags")
    private Map<String, String> tags = new LinkedHashMap<>();

    public MetricTagFilter() {
    }

    public MetricTagFilter(final String regex, final Map<String, String> tags) {
        this.regex = regex;
        this.tags = tags;
    }

    public String getRegex() {
        return regex;
    }

    public Map<String, String> getTags() {
        return tags;
    }
}
