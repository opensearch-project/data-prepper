/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue; 
import jakarta.validation.Valid;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>truncate</code> processor truncates a key's value at the beginning, the end, " +
        "or on both sides of the value string, based on the processor’s configuration.")
public class TruncateProcessorConfig {
    public static class Entry {
        @JsonProperty("source_keys")
        @JsonPropertyDescription("The list of source keys that will be modified by the processor. " +
                "The default value is an empty list, which indicates that all values will be truncated.")
        private List<String> sourceKeys;

        @JsonProperty(value = "start_at", defaultValue = "0")
        @JsonPropertyDescription("The index into the string value to start truncation. " +
                "Default is <code>0</code>, which specifies to start truncation at the beginning of each key's value.")
        private Integer startAt;

        @JsonProperty("length")
        @JsonPropertyDescription("The length of the string after truncation. " +
                "When not specified, the processor will measure the length based on where the string ends.")
        private Integer length;

        @JsonProperty("recursive")
        @JsonPropertyDescription("Recursively truncates the fields. If the value of a field is a map, then it recursively applies truncate operation on the fields in the map.")
        private Boolean recurse = false;

        @JsonProperty("truncate_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>. " +
                "If specified, the <code>truncate</code> processor will only run on events when the expression evaluates to true.")
        @ExampleValues(
                @Example(value = "length(/node_name) > 10", description = "Truncate if the value of node_name is greater than 10 characters.")
        )
        private String truncateWhen;

        public Entry(final List<String> sourceKeys, final Integer startAt, final Integer length, final String truncateWhen, final Boolean recurse) {
            this.sourceKeys = sourceKeys;
            this.startAt = startAt;
            this.length = length;
            this.truncateWhen = truncateWhen;
            this.recurse = recurse;
        }

        public Entry() {}

        public List<String> getSourceKeys() {
            return sourceKeys;
        }

        public Integer getStartAt() {
            return startAt;
        }

        public Boolean getRecurse() {
            return recurse;
        }

        public Integer getLength() {
            return length;
        }

        public String getTruncateWhen() {
            return truncateWhen;
        }

        @AssertTrue(message = "At least one of start_at or length or both must be specified and the values must be positive integers")
        public boolean isValidConfig() {
            if (length == null && startAt == null) {
                return false;
            }
            if (length != null && length < 0) {
                return false;
            }
            if (startAt != null && startAt < 0) {
                return false;
            }
            return true;
        }
    }

    @NotEmpty
    @NotNull
    @JsonPropertyDescription("A list of entries to add to an event.")
    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

}

