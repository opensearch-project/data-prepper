/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class RenameKeyProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        private String fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        private String toKey;

        @JsonProperty("overwrite_if_to_key_exists")
        private boolean overwriteIfToKeyExists = false;

        @JsonProperty("rename_when")
        private String renameWhen;

        public String getFromKey() {
            return fromKey;
        }

        public String getToKey() {
            return toKey;
        }

        public boolean getOverwriteIfToKeyExists() {
            return overwriteIfToKeyExists;
        }

        public String getRenameWhen() { return renameWhen; }

        public Entry(final String fromKey, final String toKey, final boolean overwriteIfKeyExists, final String renameWhen) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.overwriteIfToKeyExists = overwriteIfKeyExists;
            this.renameWhen = renameWhen;
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
