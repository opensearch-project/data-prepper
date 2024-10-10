/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder
@JsonClassDescription("Appends multiple events into a single event.")
public class AppendAggregateActionConfig {

    @JsonProperty("keys_to_append")
    @JsonPropertyDescription("A list of keys to append to for the aggregated result.")
    List<String> keysToAppend;

    public List<String> getKeysToAppend() {
        return keysToAppend;
    }

}
