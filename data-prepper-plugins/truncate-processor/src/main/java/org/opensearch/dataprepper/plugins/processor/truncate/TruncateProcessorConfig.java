/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue; 
import jakarta.validation.Valid;

import java.util.List;

@JsonClassDescription("https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/truncate/#configuration")
public class TruncateProcessorConfig {
    public static class Entry {
        @JsonProperty("source_keys")
        @JsonPropertyDescription("The list of source keys that will be modified by the processor. " +
                "The default value is an empty list, which indicates that all values will be truncated.")
        private List<String> sourceKeys;

        @JsonProperty("start_at")
        @JsonPropertyDescription("Where in the string value to start truncation. " +
                "Default is `0`, which specifies to start truncation at the beginning of each key's value.")
        private Integer startAt;

        @JsonProperty("length")
        @JsonPropertyDescription("The length of the string after truncation. " +
                "When not specified, the processor will measure the length based on where the string ends.")
        private Integer length;

        @JsonProperty("recursive")
        private Boolean recurse = false;

        @JsonProperty("truncate_when")
        @JsonPropertyDescription("A condition that, when met, determines when the truncate operation is performed.")
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

