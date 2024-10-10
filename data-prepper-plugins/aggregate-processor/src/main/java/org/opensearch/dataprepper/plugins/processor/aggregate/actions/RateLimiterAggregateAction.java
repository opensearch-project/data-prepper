/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;

import com.google.common.util.concurrent.RateLimiter;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action 
 * 
 * @since 2.1
 */
@DataPrepperPlugin(name = "rate_limiter", pluginType = AggregateAction.class, pluginConfigurationType = RateLimiterAggregateActionConfig.class)
public class RateLimiterAggregateAction implements AggregateAction {
    private final RateLimiter rateLimiter;
    private final RateLimiterMode rateLimiterMode;

    @DataPrepperPluginConstructor
    public RateLimiterAggregateAction(final RateLimiterAggregateActionConfig ratelimiterAggregateActionConfig) {
        final int eventsPerSecond = ratelimiterAggregateActionConfig.getEventsPerSecond();
        this.rateLimiterMode = ratelimiterAggregateActionConfig.getWhenExceeds();
        this.rateLimiter = RateLimiter.create(eventsPerSecond);
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        if (rateLimiterMode == RateLimiterMode.DROP) {
            if (!rateLimiter.tryAcquire()) {
                return AggregateActionResponse.nullEventResponse();
            }
        } else {
            rateLimiter.acquire();
        }
        return new AggregateActionResponse(event);
    }
}
