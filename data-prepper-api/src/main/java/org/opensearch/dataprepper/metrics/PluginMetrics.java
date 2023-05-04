/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.StringJoiner;
import java.util.function.ToDoubleFunction;

/**
 * Provides reference to APIs that register timer, counter, gauge into global registry.
 * See https://micrometer.io/docs/concepts#_registry
 */
public class PluginMetrics {

    private final String metricsPrefix;

    public static PluginMetrics fromPluginSetting(final PluginSetting pluginSetting) {
        if(pluginSetting.getPipelineName() == null) {
            throw new IllegalArgumentException("PluginSetting.pipelineName must not be null");
        }
        return PluginMetrics.fromNames(pluginSetting.getName(), pluginSetting.getPipelineName());
    }

    /**
     * Provides reference to APIs that register timer, counter, gauge into global registry.
     *
     * @param componentId It can be either pluginId or Data Prepper core component id
     * @param componentScope It can be pipeline name or Data Prepper core component
     * @return The {@link PluginMetrics}
     */
    public static PluginMetrics fromNames(final String componentId, final String componentScope) {
        return new PluginMetrics(new StringJoiner(MetricNames.DELIMITER)
                .add(componentScope)
                .add(componentId).toString());
    }

    private  PluginMetrics(final String metricsPrefix) {
        this.metricsPrefix = metricsPrefix;
    }

    public Counter counter(final String name) {
        return Metrics.counter(getMeterName(name));
    }

    public Counter counterWithTags(final String name, final String... tags) {
        return Metrics.counter(getMeterName(name), tags);
    }

    public Counter counter(final String name, final String metricsPrefix) {
        return Metrics.counter(new StringJoiner(MetricNames.DELIMITER).add(metricsPrefix).add(name).toString());
    }

    public Timer timer(final String name) {
        return Metrics.timer(getMeterName(name));
    }

    public Timer timerWithTags(final String name, final String... tags) {
        return Metrics.timer(getMeterName(name), tags);
    }

    public DistributionSummary summary(final String name) {
        return Metrics.summary(getMeterName(name));
    }

    public <T extends Number> T gauge(final String name, T number) {
        return Metrics.gauge(getMeterName(name), number);
    }

    public <T> T gauge(String name, T obj, ToDoubleFunction<T> valueFunction) {
        return Metrics.gauge(getMeterName(name), obj, valueFunction);
    }

    private String getMeterName(final String name) {
        return new StringJoiner(MetricNames.DELIMITER).add(metricsPrefix).add(name).toString();
    }

}
