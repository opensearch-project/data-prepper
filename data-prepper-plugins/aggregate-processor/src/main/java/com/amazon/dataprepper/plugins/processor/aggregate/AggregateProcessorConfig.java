/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class AggregateProcessorConfig {

    static int DEFAULT_GROUP_DURATION = 180;

    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonProperty("group_duration")
    @Min(0)
    private int groupDuration = DEFAULT_GROUP_DURATION;

    @JsonProperty("action")
    @NotNull
    private PluginModel aggregateAction;

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }

    public int getGroupDuration() {
        return groupDuration;
    }

    public PluginModel getAggregateAction() { return aggregateAction; }

}
