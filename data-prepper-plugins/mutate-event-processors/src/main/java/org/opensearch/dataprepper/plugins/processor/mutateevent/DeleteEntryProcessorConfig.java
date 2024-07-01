/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.List;

public class DeleteEntryProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    @EventKeyConfiguration(EventKeyFactory.EventAction.DELETE)
    private List<@NotNull @NotEmpty EventKey> withKeys;

    @JsonProperty("delete_when")
    private String deleteWhen;

    public List<EventKey> getWithKeys() {
        return withKeys;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }
}
