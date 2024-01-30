/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class EntryConfig {
    static final String DEFAULT_TARGET = "geo";
    @JsonProperty("source")
    @NotEmpty
    private String source;

    @JsonProperty("target")
    private String target = DEFAULT_TARGET;

    @JsonProperty("include_fields")
    private List<String> includeFields;

    @JsonProperty("exclude_fields")
    private List<String> excludeFields;

    @JsonProperty("geoip_when")
    private String whenCondition;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public List<String> getIncludeFields() {
        return includeFields;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    @AssertTrue(message = "include_fields and exclude_fields are mutually exclusive. include_fields or exclude_fields is required.")
    boolean areFieldsValid() {
        if (includeFields == null && excludeFields == null) {
            return false;
        }
        return includeFields == null || excludeFields == null;
    }

    public String getWhenCondition() {
        return whenCondition;
    }
}
