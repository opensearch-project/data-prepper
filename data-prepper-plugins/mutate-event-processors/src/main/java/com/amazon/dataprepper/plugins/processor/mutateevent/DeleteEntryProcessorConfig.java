/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class DeleteEntryProcessorConfig {
    public static class Entry {
        @NotEmpty
        @JsonProperty("with_key")
        private String withKey;

        public String getWithKey() {
            return withKey;
        }

        public Entry(final String withKey) {
            this.withKey = withKey;
        }
    }

    @NotEmpty
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
