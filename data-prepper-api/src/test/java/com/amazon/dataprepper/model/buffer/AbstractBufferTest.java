package com.amazon.dataprepper.model.buffer;

import com.amazon.dataprepper.model.CheckpointState;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.record.Record;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.Assert;
import org.junit.Test;

public class AbstractBufferTest {

    @Test
    public void testReadAndWriteMetrics() throws TimeoutException {
        // Given
        final String bufferName = "testBuffer";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();

        PluginSetting pluginSetting = new PluginSetting(bufferName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferImpl(pluginSetting);
        for(int i=0; i<5; i++) {
            abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000);
        }
        // When
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = abstractBuffer.read(1000);

        // Then
        final List<Measurement> recordsWrittenMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.RECORDS_WRITTEN).toString());
        final List<Measurement> recordsReadMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.RECORDS_READ).toString());
        final List<Measurement> recordsInFlightMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.RECORDS_INFLIGHT).toString());
        final List<Measurement> recordsProcessedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(MetricNames.RECORDS_PROCESSED).toString());
        final List<Measurement> writeTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.WRITE_TIME_ELAPSED).toString());
        final List<Measurement> readTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.READ_TIME_ELAPSED).toString());
        final List<Measurement> checkpointTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.CHECKPOINT_TIME_ELAPSED).toString());
        Assert.assertEquals(1, recordsWrittenMeasurements.size());
        Assert.assertEquals(5.0, recordsWrittenMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(1, recordsReadMeasurements.size());
        Assert.assertEquals(5.0, recordsReadMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(1, recordsInFlightMeasurements.size());
        Assert.assertEquals(5, abstractBuffer.getRecordsInFlight());
        final Measurement recordsInFlightMeasurement = recordsInFlightMeasurements.get(0);
        Assert.assertEquals(5.0, recordsInFlightMeasurement.getValue(), 0);
        Assert.assertEquals(1, recordsProcessedMeasurements.size());
        final Measurement recordsProcessedMeasurement = recordsProcessedMeasurements.get(0);
        Assert.assertEquals(0.0, recordsProcessedMeasurement.getValue(), 0);
        Assert.assertEquals(5.0, MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(
                MetricsTestUtil.isBetween(
                        MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                        0.5,
                        0.6));
        Assert.assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(readTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(readTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.1,
                0.2));
        Assert.assertEquals(0.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertEquals(0.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.TOTAL_TIME).getValue(), 0);

        // When
        abstractBuffer.checkpoint(readResult.getValue());

        // Then
        Assert.assertEquals(0, abstractBuffer.getRecordsInFlight());
        Assert.assertEquals(0.0, recordsInFlightMeasurement.getValue(), 0);
        Assert.assertEquals(5.0, recordsProcessedMeasurement.getValue(), 0);
        Assert.assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.0,
                0.001));
    }

    @Test
    public void testTimeoutMetric() throws TimeoutException {
        final String bufferName = "testBuffer";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        PluginSetting pluginSetting = new PluginSetting(bufferName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferTimeoutImpl(pluginSetting);
        Assert.assertThrows(TimeoutException.class, () -> abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000));

        final List<Measurement> timeoutMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(bufferName).add(MetricNames.WRITE_TIMEOUTS).toString());
        Assert.assertEquals(1, timeoutMeasurements.size());
        Assert.assertEquals(1.0, timeoutMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testRuntimeException() {
        final String bufferName = "testBuffer";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferNpeImpl(bufferName, pipelineName);
        Assert.assertThrows(NullPointerException.class, () -> abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000));
    }

    public static class AbstractBufferImpl extends AbstractBuffer<Record<String>> {
        private final Queue<Record<String>> queue;
        public AbstractBufferImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
            queue = new LinkedList<>();
        }

        public AbstractBufferImpl(final String name, final String pipelineName) {
            super(name, pipelineName);
            queue = new LinkedList<>();
        }

        @Override
        public void doWrite(Record<String> record, int timeoutInMillis) throws TimeoutException {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
            queue.add(record);
        }

        @Override
        public Map.Entry<Collection<Record<String>>, CheckpointState> doRead(int timeoutInMillis) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
            final Collection<Record<String>> records = new HashSet<>();
            for(int i=0; i<5; i++) {
                if(!queue.isEmpty()) {
                    records.add(queue.remove());
                }
            }
            final CheckpointState checkpointState = new CheckpointState(records.size());
            return new AbstractMap.SimpleEntry<>(records, checkpointState);
        }

        @Override
        public void doCheckpoint(final CheckpointState checkpointState) {

        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    }

    public static class AbstractBufferTimeoutImpl extends AbstractBufferImpl {
        public AbstractBufferTimeoutImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doWrite(Record<String> record, int timeoutInMillis) throws TimeoutException {
            throw new TimeoutException();
        }
    }

    public static class AbstractBufferNpeImpl extends AbstractBufferImpl {
        public AbstractBufferNpeImpl(final String name, final String pipelineName) {
            super(name, pipelineName);
        }

        @Override
        public void doWrite(Record<String> record, int timeoutInMillis) throws TimeoutException {
            throw new NullPointerException();
        }
    }
}
