/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PluginMetricsTest {
    private static final String PLUGIN_NAME = "testPlugin";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final String TAG_KEY = "tagKey";
    private static final String TAG_VALUE = "tagValue";
    private PluginMetrics objectUnderTest;
    private PluginSetting pluginSetting;

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);

        objectUnderTest = PluginMetrics.fromPluginSetting(pluginSetting);
    }

    @Test
    public void testCounter() {
        final Counter counter = objectUnderTest.counter("counter");
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("counter").toString(),
                counter.getId().getName());
    }

    @Test
    public void testCounterWithTags() {
        final Counter counter = objectUnderTest.counterWithTags("counter", TAG_KEY, TAG_VALUE);
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("counter").toString(),
                counter.getId().getName());

        assertEquals(TAG_VALUE, counter.getId().getTag(TAG_KEY));
    }

    @Test
    public void testCustomMetricsPrefixCounter() {
        final Counter counter = objectUnderTest.counter("counter", PIPELINE_NAME);
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add("counter").toString(),
                counter.getId().getName());
    }

    @Test
    public void testTimer() {
        final Timer timer = objectUnderTest.timer("timer");
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("timer").toString(),
                timer.getId().getName());
    }

    @Test
    public void testTimerWithTags() {
        final Timer timer = objectUnderTest.timerWithTags("timer", TAG_KEY, TAG_VALUE);
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("timer").toString(),
                timer.getId().getName());

        assertEquals(TAG_VALUE, timer.getId().getTag(TAG_KEY));
    }

    @Test
    public void testSummary() {
        final DistributionSummary summary = objectUnderTest.summary("summary");
        assertEquals(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("summary").toString(),
                summary.getId().getName());
    }

    @Test
    public void testNumberGauge() {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final AtomicInteger gauge = objectUnderTest.gauge("gauge", atomicInteger);
        assertNotNull(
                Metrics.globalRegistry.get(new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("gauge").toString()).meter());
        assertEquals(atomicInteger.get(), gauge.get());
    }

    @Test
    public void testReferenceGauge() {
        final String testString = "abc";
        final String gauge = objectUnderTest.gauge("gauge", testString, String::length);
        assertNotNull(
                Metrics.globalRegistry.get(new StringJoiner(MetricNames.DELIMITER)
                        .add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("gauge").toString()).meter());
        assertEquals(3, gauge.length());
    }

    @Test
    public void testEmptyPipelineName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PluginMetrics.fromPluginSetting(new PluginSetting("badSetting", Collections.emptyMap())));
    }
}
