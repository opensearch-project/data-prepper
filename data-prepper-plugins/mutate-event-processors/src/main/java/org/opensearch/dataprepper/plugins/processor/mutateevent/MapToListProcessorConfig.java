/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class MapToListProcessorConfig {
    private static final String DEFAULT_KEY_NAME = "key";
    private static final String DEFAULT_VALUE_NAME = "value";

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source;

    @NotEmpty
    @NotNull
    @JsonProperty("target")
    private String target;

    @JsonProperty("key_name")
    private String keyName = DEFAULT_KEY_NAME;

    @JsonProperty("value_name")
    private String valueName = DEFAULT_VALUE_NAME;

    @JsonProperty("map_to_list_when")
    private String mapToListWhen;

    public String getSource() {
        return source;
    }
    public String getTarget() {
        return target;
    }

    public String getKeyName() {
        return keyName;
    }

    public String getValueName() {
        return valueName;
    }

    public String getMapToListWhen() {
        return mapToListWhen;
    }
}
