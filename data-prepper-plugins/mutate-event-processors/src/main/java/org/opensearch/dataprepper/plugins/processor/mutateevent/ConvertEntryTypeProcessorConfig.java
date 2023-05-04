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
    private TargetType type = TargetType.INTEGER;

    @JsonProperty("convert_when")
    private String convertWhen;

    public String getKey() {
        return key;
    }

    public TargetType getType() {
        return type;
    }

    public String getConvertWhen() { return convertWhen; }
}
