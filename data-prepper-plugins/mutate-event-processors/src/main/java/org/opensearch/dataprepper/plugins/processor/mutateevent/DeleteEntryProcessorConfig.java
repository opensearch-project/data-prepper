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
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.IfThenElse;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired.SchemaProperty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

@ConditionalRequired(value = {
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "entries", value = "null")},
                thenExpect = {@SchemaProperty(field = "with_keys")}
        ),
        @IfThenElse(
                ifFulfilled = {@SchemaProperty(field = "with_keys", value = "null")},
                thenExpect = {@SchemaProperty(field = "entries")}
        )
})
@JsonPropertyOrder
@JsonClassDescription("The <code>delete_entries</code> processor deletes fields from events. " +
        "You can define the keys you want to delete in the <code>with_keys</code> configuration. " +
        "Those keys and their values are deleted from events.")
public class DeleteEntryProcessorConfig {

    @JsonPropertyOrder
    public static class Entry {
        @NotEmpty
        @NotNull
        @JsonProperty("with_keys")
        @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
        @JsonPropertyDescription("A list of keys to be deleted.")
        private List<@NotNull @NotEmpty EventKey> withKeys;

        @JsonProperty("delete_when")
        @JsonPropertyDescription("Specifies under what condition the deletion should be performed. " +
                "By default, keys are always deleted. Example: <code>/mykey == \"---\"</code>")
        @ExampleValues({
                @Example(value = "/some_key == null", description = "Only runs the deletion if the key some_key is null or does not exist.")
        })
        private String deleteWhen;

        public List<EventKey> getWithKeys() {
            return withKeys;
        }

        public String getDeleteWhen() {
            return deleteWhen;
        }

        public Entry(final List<EventKey> withKeys, final String deleteWhen) {
            this.withKeys = withKeys;
            this.deleteWhen = deleteWhen;
        }

        public Entry() {

        }
    }

    @JsonProperty("with_keys")
    @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
    @JsonPropertyDescription("A list of keys to be deleted (legacy format).")
    private List<@NotNull @NotEmpty EventKey> withKeys;

    @JsonProperty("delete_when")
    @JsonPropertyDescription("Specifies under what condition the deletion should be performed (legacy format).")
    private String deleteWhen;

    @Valid
    @JsonProperty("entries")
    @JsonPropertyDescription("A list of entries to delete from the event.")
    private List<Entry> entries;

    @AssertTrue(message = "Either 'entries' or 'with_keys' must be specified, but neither was found")
    boolean isConfigurationPresent() {
        return entries != null || withKeys != null;
    }

    @AssertFalse(message = "Either use 'entries' OR 'with_keys' with 'delete_when' configuration, but not both")
    boolean hasBothConfigurations() {
        return entries != null && withKeys != null;
    }

    public List<EventKey> getWithKeys() {
        return withKeys;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
