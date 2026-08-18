/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;

import com.google.common.util.concurrent.RateLimiter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An AggregateAction that rate limits the number of events processed per second. Rate limits may be
 * applied per group by configuring {@code events_per_second_by_group}, keyed by the {@code aggregate}
 * processor's {@code identification_keys}. Groups without an explicit configuration are each rate
 * limited at the default {@code events_per_second}.
 *
 * @since 2.1
 */
@DataPrepperPlugin(name = "rate_limiter", pluginType = AggregateAction.class, pluginConfigurationType = RateLimiterAggregateActionConfig.class)
public class RateLimiterAggregateAction implements AggregateAction {
    private final int defaultEventsPerSecond;
    private final RateLimiterMode rateLimiterMode;
    private final Map<Map<Object, Object>, RateLimiter> configuredRateLimiterByGroup;
    private final Map<Map<Object, Object>, RateLimiter> defaultRateLimiterByGroup;

    @DataPrepperPluginConstructor
    public RateLimiterAggregateAction(final RateLimiterAggregateActionConfig ratelimiterAggregateActionConfig) {
        this.defaultEventsPerSecond = ratelimiterAggregateActionConfig.getEventsPerSecond();
        this.rateLimiterMode = ratelimiterAggregateActionConfig.getWhenExceeds();
        this.defaultRateLimiterByGroup = new ConcurrentHashMap<>();
        this.configuredRateLimiterByGroup = new HashMap<>();
        for (final RateLimiterAggregateActionConfig.GroupRateLimit groupRateLimit : ratelimiterAggregateActionConfig.getEventsPerSecondByGroup()) {
            configuredRateLimiterByGroup.put(new HashMap<>(groupRateLimit.getKey()), RateLimiter.create(groupRateLimit.getEventsPerSecond()));
        }
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final RateLimiter rateLimiter = getRateLimiterForGroup(aggregateActionInput.getIdentificationKeys());
        if (rateLimiterMode == RateLimiterMode.DROP) {
            if (!rateLimiter.tryAcquire()) {
                return AggregateActionResponse.nullEventResponse();
            }
        } else {
            rateLimiter.acquire();
        }
        return new AggregateActionResponse(event);
    }

    private RateLimiter getRateLimiterForGroup(final Map<Object, Object> identificationKeys) {
        final RateLimiter configuredRateLimiter = configuredRateLimiterByGroup.get(identificationKeys);
        return Objects.requireNonNullElseGet(configuredRateLimiter, () ->
                defaultRateLimiterByGroup.computeIfAbsent(identificationKeys, key -> RateLimiter.create(defaultEventsPerSecond)));
    }
}
