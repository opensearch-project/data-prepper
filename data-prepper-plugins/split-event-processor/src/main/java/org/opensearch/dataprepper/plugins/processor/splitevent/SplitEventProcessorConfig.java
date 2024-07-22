/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.splitevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size; 


public class SplitEventProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("field")
    @JsonPropertyDescription("The event field to be split")
    private String field;

    @JsonProperty("delimiter_regex")
    @JsonPropertyDescription("The regular expression used as the delimiter for splitting the field")
    private String delimiterRegex;

    @Size(min = 1, max = 1)
    @JsonPropertyDescription("The delimiter used for splitting the field. If not specified, the default delimiter is used")
    private String delimiter;

    public String getField() {
        return field;
    }

    public String getDelimiterRegex() {
        return delimiterRegex;
    }

    public String getDelimiter() {
        return delimiter;
    }
}
