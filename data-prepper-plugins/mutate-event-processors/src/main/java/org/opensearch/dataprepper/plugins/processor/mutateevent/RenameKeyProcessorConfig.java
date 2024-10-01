/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `rename_keys` processor renames keys in an event.")
public class RenameKeyProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        @JsonPropertyDescription("The key of the entry to be renamed.")
        @EventKeyConfiguration({EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.DELETE})
        private EventKey fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        @JsonPropertyDescription("The new key of the entry.")
        @EventKeyConfiguration(EventKeyFactory.EventAction.PUT)
        private EventKey toKey;

        @JsonProperty("overwrite_if_to_key_exists")
        @JsonPropertyDescription("When set to true, the existing value is overwritten if key already exists in the event. The default value is false.")
        private boolean overwriteIfToKeyExists = false;

        @JsonProperty("rename_when")
        @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
                "such as `/some-key == \"test\"'`, that will be evaluated to determine whether the processor will be " +
                "run on the event. Default is `null`. All events will be processed unless otherwise stated.")
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
