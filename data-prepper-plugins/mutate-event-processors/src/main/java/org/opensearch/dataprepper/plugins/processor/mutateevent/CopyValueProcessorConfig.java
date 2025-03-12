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
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>copy_values</code> processor copies values from an event to another key in an event.")
public class CopyValueProcessorConfig {
    static final String FROM_LIST_KEY = "from_list";
    static final String TO_LIST_KEY = "to_list";

    @JsonPropertyOrder
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("from_key")
        @JsonPropertyDescription("The key of the entry to be copied. This must be configured.")
        private String fromKey;

        @NotEmpty
        @NotNull
        @JsonProperty("to_key")
        @JsonPropertyDescription("The key of the new entry to be added. This must be configured.")
        private String toKey;

        @JsonProperty("overwrite_if_to_key_exists")
        @JsonPropertyDescription("When set to <code>true</code>, the existing value is overwritten if <code>key</code> already exists in " +
                "the event. The default value is <code>false</code>.")
        private boolean overwriteIfToKeyExists = false;

        @JsonProperty("copy_when")
        @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
                "such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be run on the event.")
        @ExampleValues({
                @Example(value = "/some_key != null", description = "Only runs the copy_values processor on the Event if the existing key some_key is not null."),
                @Example(value = "/some_key typeof integer", description = "Only runs the copy_values processor on the Event if the key some_key is an integer.")
        })
        private String copyWhen;

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

    @JsonProperty(FROM_LIST_KEY)
    @JsonPropertyDescription("The key of the list of objects to be copied. " +
            "Both <code>from_key</code> and <code>to_key</code> must be configured and will be applied on the corresponding list.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = TO_LIST_KEY)
    })
    private String fromList;

    @JsonProperty(TO_LIST_KEY)
    @JsonPropertyDescription("The key of the new list to be added. " +
            "Both <code>from_key</code> and <code>to_key</code> must be configured and will be applied on the corresponding list.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = FROM_LIST_KEY)
    })
    private String toList;

    @JsonProperty("overwrite_if_to_list_exists")
    @JsonPropertyDescription("When set to <code>true</code>, the existing value is overwritten if <code>key</code> already exists in the event. The default value is <code>false</code>.")
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
