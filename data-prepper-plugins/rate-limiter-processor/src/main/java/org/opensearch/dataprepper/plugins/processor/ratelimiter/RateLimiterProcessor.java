/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ratelimiter;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@DataPrepperPlugin(name = "rate_limiter", pluginType = Processor.class, pluginConfigurationType = RateLimiterProcessorConfig.class)
public class RateLimiterProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final int eventsPerSecond;
    private final int counterRetentionSeconds;
    private final ConcurrentHashMap<Long, AtomicInteger> eventCountPerSecond = new ConcurrentHashMap<>();

    @DataPrepperPluginConstructor
    public RateLimiterProcessor(final PluginMetrics pluginMetrics,
                                final RateLimiterProcessorConfig config) {
        super(pluginMetrics);
        this.eventsPerSecond = config.getEventsPerSecond();
        this.counterRetentionSeconds = config.getCounterRetentionSeconds();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final Collection<Record<Event>> output = new ArrayList<>();

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final long arrivalSecond = event.getMetadata().getTimeReceived().getEpochSecond();

            final AtomicInteger count = eventCountPerSecond.computeIfAbsent(arrivalSecond, k -> new AtomicInteger(0));
            if (count.incrementAndGet() <= eventsPerSecond) {
                output.add(record);
            }
        }

        evictExpiredCounters();

        return output;
    }

    private void evictExpiredCounters() {
        final long now = Instant.now().getEpochSecond();
        eventCountPerSecond.keySet().removeIf(second -> now - second > counterRetentionSeconds);
    }


    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
