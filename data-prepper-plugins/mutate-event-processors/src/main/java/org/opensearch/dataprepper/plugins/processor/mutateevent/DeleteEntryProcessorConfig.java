/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>delete_entries</code> processor deletes fields from events. " +
        "You can define the keys you want to delete in the <code>with_keys</code> configuration. " +
        "Those keys and their values are deleted from events.")
public class DeleteEntryProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
    @JsonPropertyDescription("A list of keys to be deleted.")
    private List<@NotNull @NotEmpty EventKey> withKeys;

    @JsonProperty("delete_when")
    @JsonPropertyDescription("Specifies under what condition the <code>delete_entries</code> processor should perform deletion. " +
            "By default, keys are always deleted. Example: <code>/mykey == \"---\"</code>")
    private String deleteWhen;

    public List<EventKey> getWithKeys() {
        return withKeys;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }
}
