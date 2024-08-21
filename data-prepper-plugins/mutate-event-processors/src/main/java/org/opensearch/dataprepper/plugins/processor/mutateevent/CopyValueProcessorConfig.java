/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `copy_values` processor copies values within an event and is a [mutate event]" +
        "(https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/mutate-event/) processor.")
public class CopyValueProcessorConfig {
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        @JsonPropertyDescription("The key of the entry to be copied.")
        private String fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        @JsonPropertyDescription("The key of the new entry to be added.")
        private String toKey;

        @JsonProperty("copy_when")
        @JsonPropertyDescription("A [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
                "such as `/some-key == \"test\"'`, that will be evaluated to determine whether the processor will be run on the event.")
        private String copyWhen;

        @JsonProperty("overwrite_if_to_key_exists")
        @JsonPropertyDescription("When set to `true`, the existing value is overwritten if `key` already exists in " +
                "the event. The default value is `false`.")
        private boolean overwriteIfToKeyExists = false;

        public String getFromKey() {
            return fromKey;
        }

        public String getToKey() {
            return toKey;
        }

        public boolean getOverwriteIfToKeyExists() {
            return overwriteIfToKeyExists;
        }

        public String getCopyWhen() { return copyWhen; }

        public Entry(final String fromKey, final String toKey, final boolean overwriteIfToKeyExists, final String copyWhen) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.overwriteIfToKeyExists = overwriteIfToKeyExists;
            this.copyWhen = copyWhen;
        }

        public Entry() {

        }
    }

    @NotEmpty
    @NotNull
    @Valid
    @JsonPropertyDescription("A list of entries to be copied in an event.")
    private List<Entry> entries;

    @JsonProperty("from_list")
    @JsonPropertyDescription("The source list to copy values from.")
    private String fromList;

    @JsonProperty("to_list")
    @JsonPropertyDescription("The target list to copy values to.")
    private String toList;

    @JsonProperty("overwrite_if_to_list_exists")
    @JsonPropertyDescription("When set to `true`, the existing value is overwritten if `key` already exists in the event. The default value is `false`.")
    private boolean overwriteIfToListExists = false;

    @AssertTrue(message = "Both from_list and to_list should be specified when copying entries between lists.")
    boolean isBothFromListAndToListProvided() {
        return (fromList == null && toList == null) || (fromList != null && toList != null);
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public String getFromList() {
        return fromList;
    }

    public String getToList() {
        return toList;
    }

    public boolean getOverwriteIfToListExists() {
        return overwriteIfToListExists;
    }
}
