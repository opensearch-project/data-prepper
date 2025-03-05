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
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ValidRegex;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "delimiter", value = "null")},
                thenExpect = {@SchemaProperty(field = "delimiter_regex")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "delimiter_regex", value = "null")},
                thenExpect = {@SchemaProperty(field = "delimiter")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>split_event</code> processor is used to split events based on a delimiter and " +
        "generates multiple events from a user-specified field.")
public class SplitEventProcessorConfig {
    static final String DELIMITER_REGEX_KEY = "delimiter_regex";

    @NotEmpty
    @NotNull
    @JsonProperty("field")
    @JsonPropertyDescription("The event field to be split.")
    private String field;

    @Size(min = 1, max = 1)
    @JsonPropertyDescription("The delimiter character used for splitting the field. You must provide either the <code>delimiter</code> or the <code>delimiter_regex</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name=DELIMITER_REGEX_KEY, allowedValues = {"null", "\"\""})
    })
    private String delimiter;

    @ValidRegex(message = "The value of delimiter_regex is not a valid regex string")
    @JsonProperty(DELIMITER_REGEX_KEY)
    @JsonPropertyDescription("The regular expression used as the delimiter for splitting the field. You must provide either the <code>delimiter</code> or the <code>delimiter_regex</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name="delimiter", allowedValues = {"null", "\"\""})
    })
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
