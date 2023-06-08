/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;
import java.util.Optional;

public class ConvertEntryTypeProcessorConfig  {
    @JsonProperty("key")
    @NotEmpty
    private String key;

    @JsonProperty("type")
    private TargetType type = TargetType.INTEGER;

    @JsonProperty("convert_when")
    private String convertWhen;

    @JsonProperty("null_values")
    private List<String> nullValues;

    public String getKey() {
        return key;
    }

    public TargetType getType() {
        return type;
    }

    public String getConvertWhen() { return convertWhen; }

    public Optional<List<String>> getNullValues(){
        return Optional.ofNullable(nullValues);
    }
}
