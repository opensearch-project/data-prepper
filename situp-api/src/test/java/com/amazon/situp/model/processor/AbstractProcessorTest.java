package com.amazon.situp.model.processor;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.metrics.MetricNames;
import com.amazon.situp.model.record.Record;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Assert;
import org.junit.Test;

public class AbstractProcessorTest {

    public static class ProcessorImpl extends AbstractProcessor<Record<String>, Record<String>> {
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

    @Test
    public void testMetrics() {
        final String processorName = "testProcessor";
        final String pipelineName = "pipelineName";
        final SimpleMeterRegistry simple = new SimpleMeterRegistry();
        Metrics.addRegistry(simple);

        PluginSetting pluginSetting = new PluginSetting(processorName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractProcessor<Record<String>, Record<String>> processor = new ProcessorImpl(pluginSetting);

        processor.execute(Arrays.asList(
                new Record<>("Value1"),
                new Record<>("Value2"),
                new Record<>("Value3")
        ));

        final List<Measurement> recordsInMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.RECORDS_IN).toString(),
                simple);
        final List<Measurement> recordsOutMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.RECORDS_OUT).toString(),
                simple);
        final List<Measurement> elapsedTimeMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(processorName).add(MetricNames.TIME_ELAPSED).toString(),
                simple);

        Assert.assertEquals(1, recordsInMeasurements.size());
        Assert.assertEquals(3.0, recordsInMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(1, recordsOutMeasurements.size());
        Assert.assertEquals(6.0, recordsOutMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(3, elapsedTimeMeasurements.size());
        Assert.assertEquals(1.0, getMeasurementFromList(elapsedTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(0.1 < getMeasurementFromList(elapsedTimeMeasurements, Statistic.TOTAL_TIME).getValue());
        Assert.assertTrue(0.1 < getMeasurementFromList(elapsedTimeMeasurements, Statistic.MAX).getValue());
    }

    final List<Measurement> getMeasurementList(final String meterName, final MeterRegistry registry) {
        return StreamSupport.stream(registry.find(meterName).meter().measure().spliterator(), false)
                .collect(Collectors.toList());
    }

    final Measurement getMeasurementFromList(final List<Measurement> measurements, final Statistic statistic) {
        return measurements.stream().filter(measurement -> measurement.getStatistic() == statistic).findAny().get();
    }

}
