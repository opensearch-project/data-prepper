package com.amazon.dataprepper.metrics;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.Assert;
import org.junit.Test;

public class PluginMetricsTest {
    private static final String PLUGIN_NAME = "testPlugin";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final PluginSetting PLUGIN_SETTING = new PluginSetting(PLUGIN_NAME, Collections.emptyMap()) {{
        setPipelineName(PIPELINE_NAME);
    }};
    private static final PluginMetrics PLUGIN_METRICS = PluginMetrics.fromPluginSetting(PLUGIN_SETTING);

    @Test
    public void testCounter() {
        final Counter counter = PLUGIN_METRICS.counter("counter");
        Assert.assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("counter").toString(),
                counter.getId().getName());
    }

    @Test
    public void testTimer() {
        final Timer counter = PLUGIN_METRICS.timer("timer");
        Assert.assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("timer").toString(),
                counter.getId().getName());
    }

    @Test
    public void testSummary() {
        final DistributionSummary summary = PLUGIN_METRICS.summary("summary");
        Assert.assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("summary").toString(),
                summary.getId().getName());
    }

    @Test
    public void testNumberGauge() {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final AtomicInteger gauge = PLUGIN_METRICS.gauge("gauge", atomicInteger);
        Assert.assertNotNull(
                Metrics.globalRegistry.get(new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("gauge").toString()).meter());
        Assert.assertEquals(atomicInteger.get(), gauge.get());
    }

    @Test
    public void testReferenceGauge() {
        final String testString = "abc";
        final String gauge = PLUGIN_METRICS.gauge("gauge", testString, String::length);
        Assert.assertNotNull(
                Metrics.globalRegistry.get(new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("gauge").toString()).meter());
        Assert.assertEquals(3, gauge.length());
    }

    @Test
    public void testEmptyPipelineName() {
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PluginMetrics.fromPluginSetting(new PluginSetting("badSetting", Collections.emptyMap())));
    }
}
