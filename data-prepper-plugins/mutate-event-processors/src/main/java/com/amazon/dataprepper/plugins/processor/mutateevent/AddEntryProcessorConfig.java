/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class AddEntryProcessorConfig {
    public static class Entry {
        @NotEmpty
        private final String key;

        @NotEmpty
        private final Object value;

        @JsonProperty("overwrite_if_key_exists")
        private boolean overwriteIfKeyExists = false;

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public boolean getOverwriteIfKeyExists() {
            return overwriteIfKeyExists;
        }

        public Entry(final String key, final Object value, final boolean overwriteIfKeyExists)
        {
            this.key = key;
            this.value = value;
            this.overwriteIfKeyExists = overwriteIfKeyExists;
        }
    }

    @NotEmpty
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
