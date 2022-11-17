/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.typeconversion;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class TypeConversionProcessorConfig {
    @JsonProperty("key")
    @NotEmpty
    private String key;

    @JsonProperty("type")
    @NotEmpty
    private String type;

    public String getKey() {
        return key;
    }

    public String getType() {
        return type;
    }
}
