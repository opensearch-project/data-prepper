/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class DeleteEntryProcessorConfig {
    @NotEmpty
    @JsonProperty("with_key")
    private String withKey;

    public String getWithKey() {
        return withKey;
    }
}
