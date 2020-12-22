package com.amazon.dataprepper.metrics;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import java.util.StringJoiner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class PluginMetrics {

    private final String metricsPrefix;

    public static PluginMetrics fromPluginSetting(final PluginSetting pluginSetting) {
        if(pluginSetting.getPipelineName() == null) {
            throw new IllegalArgumentException("PluginSetting.pipelineName must not be null");
        }
        return PluginMetrics.fromNames(pluginSetting.getName(), pluginSetting.getPipelineName());
    }

    public static PluginMetrics fromNames(final String name, final String pipelineName) {
        return new PluginMetrics(new StringJoiner(MetricNames.DELIMITER)
                .add(pipelineName)
                .add(name).toString());
    }

    private  PluginMetrics(final String metricsPrefix) {
        this.metricsPrefix = metricsPrefix;
    }

    public Counter counter(final String name) {
        return Metrics.counter(getMeterName(name));
    }

    public Timer timer(final String name) {
        return Metrics.timer(getMeterName(name));
    }

    public DistributionSummary summary(final String name) {
        return Metrics.summary(getMeterName(name));
    }

    public <T extends Number> T gauge(final String name, T number) {
        return Metrics.gauge(getMeterName(name), number);
    }

    private String getMeterName(final String name) {
        return new StringJoiner(MetricNames.DELIMITER).add(metricsPrefix).add(name).toString();
    }

}
