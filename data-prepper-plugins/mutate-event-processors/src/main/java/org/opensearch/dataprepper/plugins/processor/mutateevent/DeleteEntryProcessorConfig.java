/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class DeleteEntryProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    private String[] withKeys;

    @JsonProperty("delete_when")
    private String deleteWhen;

    public String[] getWithKeys() {
        return withKeys;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }
}
