/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.annotations.ValidRegex;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.List;

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
@JsonClassDescription("The <code>split_string</code> processor splits a field into an array using a delimiting character.")
public class SplitStringProcessorConfig implements StringProcessorConfig<SplitStringProcessorConfig.Entry> {
    @JsonPropertyOrder
    public static class Entry {
        static final String DELIMITER_REGEX_KEY = "delimiter_regex";

        @NotEmpty
        @NotNull
        @JsonPropertyDescription("The key name of the field to split.")
        private EventKey source;

        @Size(min = 1, max = 1)
        @JsonPropertyDescription("The separator character responsible for the split. " +
                "Cannot be defined at the same time as <code>delimiter_regex</code>. " +
                "At least <code>delimiter</code> or <code>delimiter_regex</code> must be defined.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name=DELIMITER_REGEX_KEY, allowedValues = {"null", "\"\""})
        })
        @ExampleValues(
                @Example(value = "/", description = "Split a file path.")
        )
        private String delimiter;

        @ValidRegex(message = "The value of delimiter_regex is not a valid regex string")
        @JsonProperty("delimiter_regex")
        @JsonPropertyDescription("The regex string responsible for the split. Cannot be defined at the same time as <code>delimiter</code>. " +
                "At least <code>delimiter</code> or <code>delimiter_regex</code> must be defined.")
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="delimiter", allowedValues = {"null", "\"\""})
        })
        @ExampleValues(
                @Example(value = "(file:///|/+)", description = "Split a file URI, treating multiple slashes as a single slash.")
        )
        private String delimiterRegex;

        @JsonProperty("split_when")
        @JsonPropertyDescription("Specifies under what condition the <code>split_string</code> processor should perform splitting. " +
                "Default is no condition.")
        @ExampleValues(
                @Example(value = "startsWith(/path, \"file://\")", description = "Split a string only if it starts with the file URI scheme.")
        )
        private String splitWhen;

        public EventKey getSource() {
            return source;
        }

        public String getDelimiterRegex() {
            return delimiterRegex;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getSplitWhen() { return splitWhen; }

        public Entry(final EventKey source, final String delimiterRegex, final String delimiter, final String splitWhen) {
            this.source = source;
            this.delimiterRegex = delimiterRegex;
            this.delimiter = delimiter;
            this.splitWhen = splitWhen;
        }

        public Entry() {}
    }

    @Override
    @JsonIgnore
    public List<Entry> getIterativeConfig() {
        return entries;
    }

    @JsonPropertyDescription("List of entries. Each entry defines a split.")
    @NotNull
    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
