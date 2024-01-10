/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class MapToListProcessorConfig {
    private static final String DEFAULT_KEY_NAME = "key";
    private static final String DEFAULT_VALUE_NAME = "value";
    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();
    private static final boolean DEFAULT_REMOVE_PROCESSED_FIELDS = false;

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

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("remove_processed_fields")
    private boolean removeProcessedFields = DEFAULT_REMOVE_PROCESSED_FIELDS;

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

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public boolean getRemoveProcessedFields() {
        return removeProcessedFields;
    }
}
