/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class DeleteEntryProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("with_keys")
    private String[] withKeys;

    public String[] getWithKeys() {
        return withKeys;
    }
}
