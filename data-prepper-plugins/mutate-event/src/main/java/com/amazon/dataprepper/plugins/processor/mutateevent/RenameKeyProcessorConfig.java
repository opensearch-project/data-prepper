/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class RenameKeyProcessorConfig {
    @NotEmpty
    @JsonProperty("from_key")
    private String fromKey;

    @NotEmpty
    @JsonProperty("to_key")
    private String toKey;

    @JsonProperty("overwrite_if_key_exists")
    private boolean overwriteIfKeyExists = false;

    public String getFromKey() {
        return fromKey;
    }

    public String getToKey() {
        return toKey;
    }

    public boolean getOverwriteIfKeyExists() {
        return overwriteIfKeyExists;
    }
}
