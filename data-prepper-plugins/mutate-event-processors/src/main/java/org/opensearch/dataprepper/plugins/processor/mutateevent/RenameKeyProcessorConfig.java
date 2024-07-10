/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

public class RenameKeyProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        @EventKeyConfiguration({EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.DELETE})
        private EventKey fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        @EventKeyConfiguration(EventKeyFactory.EventAction.PUT)
        private EventKey toKey;

        @JsonProperty("overwrite_if_to_key_exists")
        private boolean overwriteIfToKeyExists = false;

        @JsonProperty("rename_when")
        private String renameWhen;

        public EventKey getFromKey() {
            return fromKey;
        }

        public EventKey getToKey() {
            return toKey;
        }

        public boolean getOverwriteIfToKeyExists() {
            return overwriteIfToKeyExists;
        }

        public String getRenameWhen() { return renameWhen; }

        public Entry(final EventKey fromKey, final EventKey toKey, final boolean overwriteIfKeyExists, final String renameWhen) {
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
