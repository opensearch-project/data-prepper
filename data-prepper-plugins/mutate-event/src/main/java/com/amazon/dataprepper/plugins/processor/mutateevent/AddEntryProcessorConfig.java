/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class AddEntryProcessorConfig {
    @NotEmpty
    private String key;

    @NotEmpty
    private Object value;

    @JsonProperty("overwrite_if_key_exists")
    private boolean overwriteIfKeyExists = false;

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public boolean getOverwriteIfKeyExists() {
        return overwriteIfKeyExists;
    }
}
