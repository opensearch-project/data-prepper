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
@JsonClassDescription("The `delete_entries` processor deletes entries, such as key-value pairs, from an event. " +
        "You can define the keys you want to delete in the `with-keys` field following `delete_entries` in the YAML " +
        "configuration file. Those keys and their values are deleted.")
public class DeleteEntryProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
    @JsonPropertyDescription("An array of keys for the entries to be deleted.")
    private List<@NotNull @NotEmpty EventKey> withKeys;

    @JsonProperty("delete_when")
    @JsonPropertyDescription("Specifies under what condition the `delete_entries` processor should perform deletion. " +
            "Default is no condition.")
    private String deleteWhen;

    public List<EventKey> getWithKeys() {
        return withKeys;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }
}
