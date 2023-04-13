/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Objects;

public class AddEntryProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        private String key;

        private Object value;

        private String format;

        @JsonProperty("overwrite_if_key_exists")
        private boolean overwriteIfKeyExists = false;

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public String getFormat() {
            return format;
        }

        public boolean getOverwriteIfKeyExists() {
            return overwriteIfKeyExists;
        }

        @AssertTrue(message = "Either value or format must be specified")
        public boolean hasValueOrFormat() {
            return Objects.nonNull(value) || Objects.nonNull(format);
        }

        public Entry(final String key, final Object value, final String format, final boolean overwriteIfKeyExists)
        {
            this.key = key;
            this.value = value;
            this.format = format;
            this.overwriteIfKeyExists = overwriteIfKeyExists;
        }

        public Entry() {

        }
    }

    @NotEmpty
    @NotNull
    @Valid
    private List<Entry> entries;

    public List<Entry> getEntries() {
        return entries;
    }
}
