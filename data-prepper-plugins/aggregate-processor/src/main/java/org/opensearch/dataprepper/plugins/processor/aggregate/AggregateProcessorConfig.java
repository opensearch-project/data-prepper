/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.List;

public class AggregateProcessorConfig {

    static int DEFAULT_GROUP_DURATION_SECONDS = 180;

    @JsonPropertyDescription("An unordered list by which to group events. Events with the same values as these keys are put into the same group. If an event does not contain one of the identification_keys, then the value of that key is considered to be equal to null. At least one identification_key is required (for example, [\"sourceIp\", \"destinationIp\", \"port\"].")
    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonPropertyDescription("The amount of time that a group should exist before it is concluded automatically. Supports ISO_8601 notation strings (\"PT20.345S\", \"PT15M\", etc.) as well as simple notation for seconds (\"60s\") and milliseconds (\"1500ms\"). Default value is 180s.")
    @JsonProperty("group_duration")
    private Duration groupDuration = Duration.ofSeconds(DEFAULT_GROUP_DURATION_SECONDS);

    @JsonPropertyDescription("The action to be performed on each group. One of the available aggregate actions must be provided, or you can create custom aggregate actions. remove_duplicates and put_all are the available actions. For more information, see Creating New Aggregate Actions.")
    @JsonProperty("action")
    @NotNull
    private PluginModel aggregateAction;

    @JsonPropertyDescription("When local_mode is set to true, the aggregation is performed locally on each Data Prepper node instead of forwarding events to a specific node based on the identification_keys using a hash function. Default is false.")
    @JsonProperty("local_mode")
    @NotNull
    private Boolean localMode = false;

    @JsonPropertyDescription("A boolean indicating if the unaggregated events should be forwarded to the next processor/sink in the chain.")
    @JsonProperty("output_unaggregated_events")
    private Boolean outputUnaggregatedEvents = false;

    @JsonPropertyDescription("Tag to be used for aggregated events to distinguish aggregated events from unaggregated events.")
    @JsonProperty("aggregated_events_tag")
    private String aggregatedEventsTag;

    @JsonPropertyDescription("A Data Prepper conditional expression (https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), such as '/some-key == \"test\"', that will be evaluated to determine whether the processor will be run on the event.")
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
