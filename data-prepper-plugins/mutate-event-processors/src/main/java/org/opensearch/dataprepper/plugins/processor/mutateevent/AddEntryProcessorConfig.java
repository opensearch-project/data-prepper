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
        private String key;

        private String metadataKey;

        private Object value;

        private String format;

        @JsonProperty("add_when")
        private String addWhen;

        @JsonProperty("overwrite_if_key_exists")
        private boolean overwriteIfKeyExists = false;

        public String getKey() {
            return key;
        }

        public String getMetadataKey() {
            return metadataKey;
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

        public String getAddWhen() { return addWhen; }

        @AssertTrue(message = "Either value or format must be specified")
        public boolean hasValueOrFormat() {
            return Objects.nonNull(value) || Objects.nonNull(format);
        }

        public Entry(final String key,
                     final String metadataKey,
                     final Object value,
                     final String format,
                     final boolean overwriteIfKeyExists,
                     final String addWhen)
        {
            if (key != null && metadataKey != null) {
                throw new IllegalArgumentException("Only one of the two - key and metadatakey - should be specified");
            }
            if (key == null && metadataKey == null) {
                throw new IllegalArgumentException("At least one of the two - key and metadatakey - must be specified");
            }
            this.key = key;
            this.metadataKey = metadataKey;
            this.value = value;
            this.format = format;
            this.overwriteIfKeyExists = overwriteIfKeyExists;
            this.addWhen = addWhen;
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
