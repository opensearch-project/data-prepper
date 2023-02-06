/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AppendAggregateActionConfig {

    @JsonProperty("keys_to_append")
    List<String> keysToAppend;

    public List<String> getKeysToAppend() {
        return keysToAppend;
    }

}
