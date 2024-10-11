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

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@JsonPropertyOrder
@JsonClassDescription("The <code>split_event</code> processor is used to split events based on a delimiter and " +
        "generates multiple events from a user-specified field.")
public class SplitEventProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("field")
    @JsonPropertyDescription("The event field to be split.")
    private String field;

    @Size(min = 1, max = 1)
    @JsonPropertyDescription("The delimiter character used for splitting the field. You must provide either the <code>delimiter</code> or the <code>delimiter_regex</code>.")
    private String delimiter;

    @JsonProperty("delimiter_regex")
    @JsonPropertyDescription("The regular expression used as the delimiter for splitting the field. You must provide either the <code>delimiter</code> or the <code>delimiter_regex</code>.")
    private String delimiterRegex;

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
