/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder
@JsonClassDescription("The <code>rate_limiter</code> action controls the number of events aggregated per second. " +
        "By default, <code>rate_limiter</code> blocks the <code>aggregate</code> processor from running if it receives more events than the configured number allowed. " +
        "You can overwrite the number events that triggers the <code>rate_limited</code> by using the <code>when_exceeds</code> configuration option.")
public class RateLimiterAggregateActionConfig {
    @JsonPropertyDescription("The number of events allowed per second.")
    @JsonProperty("events_per_second")
    @NotNull
    @Positive(message = "events_per_second must be greater than 0.")
    int eventsPerSecond;
    
    @JsonPropertyDescription("Indicates what action the <code>rate_limiter</code> takes when the number of events received is greater than the number of events allowed per second. " +
            "Default value is block, which blocks the processor from running after the maximum number of events allowed per second is reached until the next second. Alternatively, the drop option drops the excess events received in that second. Default is block")
    @JsonProperty(value = "when_exceeds", defaultValue = "block")
    RateLimiterMode whenExceedsMode = RateLimiterMode.BLOCK;

    @JsonPropertyDescription("Custom rate limits for specific groups. Each entry has a <code>key</code>, which is a map of " +
            "<code>identification_keys</code> to their values identifying the group, and an <code>events_per_second</code> value for that group. " +
            "Groups that do not match any entry use the top-level <code>events_per_second</code> as their default.")
    @JsonProperty("events_per_second_by_group")
    @Valid
    List<GroupRateLimit> eventsPerSecondByGroup = Collections.emptyList();

    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public RateLimiterMode getWhenExceeds() {
        return whenExceedsMode;
    }

    public List<GroupRateLimit> getEventsPerSecondByGroup() {
        return eventsPerSecondByGroup;
    }

    @AssertTrue(message = "Each key in events_per_second_by_group must be unique.")
    boolean isEventsPerSecondByGroupKeysUnique() {
        final Set<Map<String, Object>> seenKeys = new HashSet<>();
        for (final GroupRateLimit groupRateLimit : eventsPerSecondByGroup) {
            if (!seenKeys.add(groupRateLimit.getKey())) {
                return false;
            }
        }
        return true;
    }

    public static class GroupRateLimit {
        @JsonPropertyDescription("A map of <code>identification_keys</code> to the values identifying the group. " +
                "The keys must exactly match the <code>identification_keys</code> configured on the <code>aggregate</code> processor.")
        @JsonProperty("key")
        @NotEmpty
        Map<String, Object> key;

        @JsonPropertyDescription("The number of events allowed per second for this group.")
        @JsonProperty("events_per_second")
        @NotNull
        @Positive(message = "events_per_second must be greater than 0.")
        Integer eventsPerSecond;

        public Map<String, Object> getKey() {
            return key;
        }

        public Integer getEventsPerSecond() {
            return eventsPerSecond;
        }
    }
}
