/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class DeleteEntryProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("with_key")
        private String withKey;

        public String getWithKey() {
            return withKey;
        }

        public Entry(final String withKey) {
            this.withKey = withKey;
        }

        public Entry() {

        }
    }

    @NotEmpty
    @NotNull
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
