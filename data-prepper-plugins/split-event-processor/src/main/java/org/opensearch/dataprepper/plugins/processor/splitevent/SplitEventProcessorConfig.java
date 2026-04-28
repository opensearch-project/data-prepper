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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ValidRegex;

@JsonPropertyOrder
@JsonClassDescription("The <code>split_event</code> processor is used to split events based on a delimiter or an array field, " +
        "generating multiple events from a user-specified field.")
public class SplitEventProcessorConfig {
    static final String DELIMITER_REGEX_KEY = "delimiter_regex";

    @NotEmpty
    @JsonProperty("field")
    @JsonPropertyDescription("The event field to be split. When no delimiter is specified, the field is treated as an array and each element becomes a separate event.")
    private String field;

    @Size(min = 1, max = 1)
    @JsonPropertyDescription("The delimiter character used for splitting the field. When neither <code>delimiter</code> nor <code>delimiter_regex</code> is specified, the field is treated as an array.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name=DELIMITER_REGEX_KEY, allowedValues = {"null", "\"\""})
    })
    private String delimiter;

    @ValidRegex(message = "The value of delimiter_regex is not a valid regex string")
    @JsonProperty(DELIMITER_REGEX_KEY)
    @JsonPropertyDescription("The regular expression used as the delimiter for splitting the field. When neither <code>delimiter</code> nor <code>delimiter_regex</code> is specified, the field is treated as an array.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name="delimiter", allowedValues = {"null", "\"\""})
    })
    private String delimiterRegex;

    @AssertTrue(message = "delimiter and delimiter_regex cannot both be specified")
    private boolean isValidDelimiterConfig() {
        boolean hasDelimiter = delimiter != null && !delimiter.isEmpty();
        boolean hasRegex = delimiterRegex != null && !delimiterRegex.isEmpty();
        return !(hasDelimiter && hasRegex);
    }

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
