package com.amazon.dataprepper.model.processor;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.record.Record;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.Assert;
import org.junit.Test;

public class AbstractPrepperTest {

    @Test
    public void testMetrics() {
        final String processorName = "testProcessor";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();

        PluginSetting pluginSetting = new PluginSetting(processorName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractPrepper<Record<String>, Record<String>> processor = new ProcessorImpl(pluginSetting);

        processor.execute(Arrays.asList(
                new Record<>("Value1"),
                new Record<>("Value2"),
                new Record<>("Value3")
        ));

        final List<Measurement> recordsInMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.RECORDS_IN).toString());
        final List<Measurement> recordsOutMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.RECORDS_OUT).toString());
        final List<Measurement> elapsedTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.TIME_ELAPSED).toString());

        Assert.assertEquals(1, recordsInMeasurements.size());
        Assert.assertEquals(3.0, recordsInMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(1, recordsOutMeasurements.size());
        Assert.assertEquals(6.0, recordsOutMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(3, elapsedTimeMeasurements.size());
        Assert.assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.1,
                0.2));
    }

    public static class ProcessorImpl extends AbstractPrepper<Record<String>, Record<String>> {
        public ProcessorImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        Collection<Record<String>> doExecute(Collection<Record<String>> records) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            return records.stream()
                    .flatMap(stringRecord -> Arrays.asList(stringRecord, stringRecord).stream())
                    .collect(Collectors.toList());
        }
    }
}
