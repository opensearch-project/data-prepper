/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class KeyValueProcessorConfig {
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DESTINATION = "parsed_message";
    static final String DEFAULT_FIELD_DELIMITER_REGEX = "&";
    static final String DEFAULT_KEY_VALUE_DELIMITER_REGEX = "=";
    static final String DEFAULT_NON_MATCH_VALUE = null;
    static final String DEFAULT_PREFIX = "";

    @NotEmpty
    private String source = DEFAULT_SOURCE;

    @NotEmpty
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("field_delimiter_regex")
    @NotEmpty
    private String fieldDelimiterRegex = DEFAULT_FIELD_DELIMITER_REGEX;

    @JsonProperty("key_value_delimiter_regex")
    @NotEmpty
    private String keyValueDelimiterRegex = DEFAULT_KEY_VALUE_DELIMITER_REGEX;

    @JsonProperty("non_match_value")
    private String nonMatchValue = DEFAULT_NON_MATCH_VALUE;

    @NotNull
    private String prefix = DEFAULT_PREFIX;

    @JsonProperty("trim_key_regex")
    private String trimKeyRegex;

    @JsonProperty("trim_value_regex")
    private String trimValueRegex;

    public String getSource() {
        return source;
    }

    public String getDestination() {
        return destination;
    }

    public String getFieldDelimiterRegex() {
        return fieldDelimiterRegex;
    }

    public String getKeyValueDelimiterRegex() {
        return keyValueDelimiterRegex;
    }

    public String getNonMatchValue() {
        return nonMatchValue;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getTrimKeyRegex() {
        return trimKeyRegex;
    }

    public String getTrimValueRegex() {
        return trimValueRegex;
    }
}
