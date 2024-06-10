/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
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

    @JsonProperty("output_unaggregated_events")
    private Boolean outputUnaggregatedEvents = false;

    @JsonProperty("aggregated_events_tag")
    private String aggregatedEventsTag;

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

    public String getAggregatedEventsTag() {
        return aggregatedEventsTag;
    }

    public Boolean getOutputUnaggregatedEvents() {
        return outputUnaggregatedEvents;
    }

    public Boolean getLocalMode() {
        return localMode;
    }

    @AssertTrue(message="Aggragated Events Tag must be set when output_unaggregated_events is set")
    boolean isValidConfig() {
        return (!outputUnaggregatedEvents || (outputUnaggregatedEvents && aggregatedEventsTag != null));
    }

    public PluginModel getAggregateAction() { return aggregateAction; }

}
