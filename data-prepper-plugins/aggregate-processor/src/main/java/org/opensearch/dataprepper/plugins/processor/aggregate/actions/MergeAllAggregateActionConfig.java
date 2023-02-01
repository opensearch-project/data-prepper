/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MergeAllAggregateActionConfig {

    @JsonProperty("data_types")
    Map<String, String> dataTypes;

    public Map<String, String> getDataTypes() {
        return dataTypes;
    }

}
