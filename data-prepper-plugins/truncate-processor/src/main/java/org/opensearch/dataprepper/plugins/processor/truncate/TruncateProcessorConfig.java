/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.AssertTrue; 
import jakarta.validation.Valid;

import java.util.List;

public class TruncateProcessorConfig {
    public static class Entry {
        @JsonProperty("source_keys")
        private List<String> sourceKeys;

        @JsonProperty("start_at")
        private Integer startAt;

        @JsonProperty("length")
        private Integer length;

        @JsonProperty("recursive")
        private Boolean recurse = false;

        @JsonProperty("truncate_when")
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
    private List<@Valid Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }

}

