/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class ConvertEntryTypeProcessorConfig  {
    @JsonProperty("key")
    @NotEmpty
    private String key;

    @JsonProperty("type")
    @NotEmpty
    private TargetType type;

    public String getKey() {
        return key;
    }

    public TargetType getType() {
        return type;
    }
}
