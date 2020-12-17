package com.amazon.dataprepper.model.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.record.Record;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.Assert;
import org.junit.Test;

public class AbstractSinkTest {
    @Test
    public void testMetrics() {
        final String sinkName = "testSink";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        PluginSetting pluginSetting = new PluginSetting(sinkName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractSink<Record<String>> abstractSink = new AbstractSinkImpl(pluginSetting);
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

        }
    }
}
