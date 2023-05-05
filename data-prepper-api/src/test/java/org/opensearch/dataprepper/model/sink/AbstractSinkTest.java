/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

public class AbstractSinkTest {
    @Test
    public void testMetrics() {
        final String sinkName = "testSink";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        PluginSetting pluginSetting = new PluginSetting(sinkName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractSink<Record<String>> abstractSink = new AbstractSinkImpl(pluginSetting);
        abstractSink.initialize();
        Assert.assertEquals(abstractSink.isReady(), true);
        abstractSink.output(Arrays.asList(
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString()),
                new Record<>(UUID.randomUUID().toString())
        ));

        final List<Measurement> recordsInMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(sinkName).add(MetricNames.RECORDS_IN).toString());
        final List<Measurement> elapsedTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(sinkName).add(MetricNames.TIME_ELAPSED).toString());

        Assert.assertEquals(1, recordsInMeasurements.size());
        Assert.assertEquals(5.0, recordsInMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(3, elapsedTimeMeasurements.size());
        Assert.assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.5,
                0.6));
        Assert.assertEquals(abstractSink.getRetryThreadState(), null);
        abstractSink.shutdown();
    }

    @Test
    public void testSinkNotReady() {
        final String sinkName = "testSink";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        PluginSetting pluginSetting = new PluginSetting(sinkName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractSink<Record<String>> abstractSink = new AbstractSinkNotReadyImpl(pluginSetting);
        abstractSink.initialize();
        Assert.assertEquals(abstractSink.isReady(), false);
        Assert.assertEquals(abstractSink.getRetryThreadState(), Thread.State.RUNNABLE);
        // Do another intialize to make sure the sink is still not ready
        abstractSink.initialize();
        Assert.assertEquals(abstractSink.isReady(), false);
        while (!abstractSink.isReady()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {}
        }
        try {
            Thread.sleep(2000);
        } catch (Exception e) {}
        Assert.assertEquals(abstractSink.getRetryThreadState(), Thread.State.TERMINATED);
        abstractSink.shutdown();
    }

    private static class AbstractSinkImpl extends AbstractSink<Record<String>> {

        public AbstractSinkImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doOutput(Collection<Record<String>> records) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {

            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void doInitialize() {
        }

        @Override
        public boolean isReady() {
            return true;
        }
    }

    private static class AbstractSinkNotReadyImpl extends AbstractSink<Record<String>> {

        boolean initialized;
        int initCount;
        public AbstractSinkNotReadyImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
            initialized = false;
            initCount = 0;
        }

        @Override
        public void doOutput(Collection<Record<String>> records) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {

            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void doInitialize() {
            // make this check for smaller number so that test finishes sooner
            if (initCount++ == NUMBER_OF_RETRIES/200) {
                initialized = true;
            }
        }

        @Override
        public boolean isReady() {
            return initialized;
        }
    }
}
