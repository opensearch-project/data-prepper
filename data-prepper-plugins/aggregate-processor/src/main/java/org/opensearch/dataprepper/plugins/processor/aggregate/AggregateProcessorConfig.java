/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>aggregate</code> processor groups events based on the values of identification_keys. " +
        "Then, the processor performs an action on each group, helping reduce unnecessary log volume and " +
        "creating aggregated logs over time.")
public class AggregateProcessorConfig {
    static final int DEFAULT_GROUP_DURATION_SECONDS = 180;

    @JsonPropertyDescription("An unordered list by which to group events. Events with the same values as these keys are put into the same group. " +
            "If an event does not contain one of the <code>identification_keys</code>, then the value of that key is considered to be equal to <code>null</code>. " +
            "At least one <code>identification_key</code> is required. And example configuration is [\"sourceIp\", \"destinationIp\", \"port\"].")
    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonPropertyDescription("The action to be performed on each group. One of the available aggregate actions must be provided.")
    @JsonProperty("action")
    @NotNull
    @UsesDataPrepperPlugin(pluginType = AggregateAction.class)
    private PluginModel aggregateAction;

    @JsonPropertyDescription("The amount of time that a group should exist before it is concluded automatically. Supports ISO_8601 notation strings (\"PT20.345S\", \"PT15M\", etc.) as well as simple notation for seconds (\"60s\") and milliseconds (\"1500ms\"). Default value is 180s.")
    @JsonProperty(value = "group_duration", defaultValue = DEFAULT_GROUP_DURATION_SECONDS + "s")
    private Duration groupDuration = Duration.ofSeconds(DEFAULT_GROUP_DURATION_SECONDS);

    @JsonPropertyDescription("When <code>local_mode</code> is set to true, the aggregation is performed locally on each node instead of forwarding events to a specific node based on the <code>identification_keys</code> using a hash function. Default is false.")
    @JsonProperty("local_mode")
    @NotNull
    private Boolean localMode = false;

    @JsonPropertyDescription("A boolean indicating if the unaggregated events should be forwarded to the next processor or sink in the chain.")
    @JsonProperty("output_unaggregated_events")
    private Boolean outputUnaggregatedEvents = false;

    @JsonPropertyDescription("Tag to be used for aggregated events to distinguish aggregated events from unaggregated events.")
    @JsonProperty("aggregated_events_tag")
    private String aggregatedEventsTag;

    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, such as <code>/some-key == \"test\"</code>, that will be evaluated to determine whether the processor will be run on the event.")
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

    @AssertTrue(message="Aggregated Events Tag must be set when output_unaggregated_events is set")
    boolean isValidConfig() {
        return (!outputUnaggregatedEvents || (outputUnaggregatedEvents && aggregatedEventsTag != null));
    }

    public PluginModel getAggregateAction() { return aggregateAction; }

}
