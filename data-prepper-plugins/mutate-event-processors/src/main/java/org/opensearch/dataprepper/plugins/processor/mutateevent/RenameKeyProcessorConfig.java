/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;
import java.util.regex.Pattern;

@JsonPropertyOrder
@JsonClassDescription("The <code>rename_keys</code> processor renames keys in an event.")
public class RenameKeyProcessorConfig {
    @JsonPropertyOrder
    public static class Entry {
        @JsonProperty("from_key")
        @JsonPropertyDescription("The key of the entry to be renamed.")
        @EventKeyConfiguration({EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.DELETE})
        private EventKey fromKey;

        @JsonProperty("from_key_regex")
        @JsonPropertyDescription("The regex pattern of the key of the entry to be renamed.")
        private String fromKeyRegex;

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
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"'</code>, that will be evaluated to determine whether the processor will be " +
                "run on the event. By default, all events will be processed unless otherwise stated.")
        private String renameWhen;

        private Pattern fromKeyCompiledPattern;

        public EventKey getFromKey() {
            return fromKey;
        }

        public String getFromKeyPattern() {
            return fromKeyRegex;
        }

        public EventKey getToKey() {
            return toKey;
        }

        public boolean getOverwriteIfToKeyExists() {
            return overwriteIfToKeyExists;
        }

        public String getRenameWhen() {
            return renameWhen;
        }

        @JsonIgnore
        public Pattern getFromKeyCompiledPattern() {
            if (fromKeyRegex != null && fromKeyCompiledPattern == null) {
                fromKeyCompiledPattern = Pattern.compile(fromKeyRegex);
            }
            return fromKeyCompiledPattern;
        }

        public Entry(final EventKey fromKey, final String fromKeyPattern, final EventKey toKey, final boolean overwriteIfKeyExists, final String renameWhen) {
            this.fromKey = fromKey;
            this.fromKeyRegex = fromKeyPattern;
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
