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

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@DataPrepperPlugin(name = "rate_limiter", pluginType = Processor.class, pluginConfigurationType = RateLimiterProcessorConfig.class)
public class RateLimiterProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final int eventsPerSecond;
    private final long counterRetentionSeconds;
    private final RateLimiterMode whenExceeds;
    private final String limitWhen;
    private final ExpressionEvaluator expressionEvaluator;
    private final ConcurrentHashMap<Long, AtomicInteger> emittedPerSecond = new ConcurrentHashMap<>();

    @DataPrepperPluginConstructor
    public RateLimiterProcessor(final PluginMetrics pluginMetrics,
                                final RateLimiterProcessorConfig config,
                                final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.eventsPerSecond = config.getEventsPerSecond();
        this.counterRetentionSeconds = config.getCounterRetention().getSeconds();
        this.whenExceeds = config.getWhenExceeds();
        this.limitWhen = config.getLimitWhen();
        this.expressionEvaluator = expressionEvaluator;

        if (limitWhen != null && !expressionEvaluator.isValidExpressionStatement(limitWhen)) {
            throw new InvalidPluginConfigurationException(
                    String.format("limit_when \"%s\" is not a valid expression statement.", limitWhen));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final Collection<Record<Event>> output = new ArrayList<>();

        for (final Record<Event> record : records) {
            if (limitWhen != null && !expressionEvaluator.evaluateConditional(limitWhen, record.getData())) {
                output.add(record);
                continue;
            }

            if (whenExceeds == RateLimiterMode.DROP) {
                final long now = System.currentTimeMillis() / 1000;
                final AtomicInteger count = emittedPerSecond.computeIfAbsent(now, k -> new AtomicInteger(0));
                if (count.incrementAndGet() <= eventsPerSecond) {
                    output.add(record);
                }
            } else {
                waitForCapacity();
                output.add(record);
            }
        }

        evictOldCounters();
        return output;
    }

    private void waitForCapacity() {
        while (true) {
            final long now = System.currentTimeMillis() / 1000;
            final AtomicInteger count = emittedPerSecond.computeIfAbsent(now, k -> new AtomicInteger(0));
            if (count.incrementAndGet() <= eventsPerSecond) {
                return;
            }
            count.decrementAndGet();
            try {
                long sleepMs = 1000 - (System.currentTimeMillis() % 1000);
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void evictOldCounters() {
        final long now = System.currentTimeMillis() / 1000;
        emittedPerSecond.keySet().removeIf(second -> now - second > counterRetentionSeconds);
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
