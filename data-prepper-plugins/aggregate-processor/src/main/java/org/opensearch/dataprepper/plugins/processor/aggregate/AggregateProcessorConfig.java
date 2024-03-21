/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.List;

public class AggregateProcessorConfig {

    static int DEFAULT_GROUP_DURATION_SECONDS = 180;

    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonProperty("group_duration")
    private Duration groupDuration = Duration.ofSeconds(DEFAULT_GROUP_DURATION_SECONDS);

    @JsonProperty("action")
    @NotNull
    private PluginModel aggregateAction;

    @JsonProperty("local_mode")
    @NotNull
    private Boolean localMode = false;

    @JsonProperty("aggregate_when")
    private String whenCondition;

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }

    public Duration getGroupDuration() {
        return groupDuration;
    }

    public String getWhenCondition() {
        return whenCondition;
    }

    public Boolean getLocalMode() {
        return localMode;
    }

    public PluginModel getAggregateAction() { return aggregateAction; }

}
