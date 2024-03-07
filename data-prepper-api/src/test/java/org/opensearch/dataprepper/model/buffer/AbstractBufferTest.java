/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractBufferTest {
    private static final String BUFFER_NAME = "testBuffer";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final String TEST_MESSAGE = "testMessage";

    private PluginSetting testPluginSetting;

    @BeforeEach
    public void setUp() {
        MetricsTestUtil.initMetrics();
        testPluginSetting = new PluginSetting(BUFFER_NAME, Collections.emptyMap());
        testPluginSetting.setPipelineName(PIPELINE_NAME);
    }

    @Test
    public void testWriteMetrics() throws TimeoutException {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferImpl(testPluginSetting);

        // When
        for(int i=0; i<5; i++) {
            abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000);
        }

        // Then
        final List<Measurement> recordsWrittenMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_WRITTEN).toString());
        final List<Measurement> writeTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.WRITE_TIME_ELAPSED).toString());
        assertEquals(1, recordsWrittenMeasurements.size());
        assertEquals(5.0, recordsWrittenMeasurements.get(0).getValue(), 0);
        assertEquals(5.0, MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.COUNT).getValue(), 0);
        assertTrue(
                MetricsTestUtil.isBetween(
                        MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                        0.45,
                        0.65));
    }

    @Test
    public void testWriteAllMetrics() throws Exception {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = new ArrayList<>();
        for(int i=0; i<5; i++) {
            testRecords.add(new Record<>(UUID.randomUUID().toString()));
        }

        // When
        abstractBuffer.writeAll(testRecords, 1000);

        // Then
        final List<Measurement> recordsWrittenMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_WRITTEN).toString());
        final List<Measurement> writeTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.WRITE_TIME_ELAPSED).toString());
        assertEquals(1, recordsWrittenMeasurements.size());
        assertEquals(5.0, recordsWrittenMeasurements.get(0).getValue(), 0);
        assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.COUNT).getValue(), 0);
        assertTrue(
                MetricsTestUtil.isBetween(
                        MetricsTestUtil.getMeasurementFromList(writeTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                        0.05,
                        0.25));
    }

    @Test
    public void testReadMetrics() throws Exception {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = new ArrayList<>();
        for(int i=0; i<5; i++) {
            testRecords.add(new Record<>(UUID.randomUUID().toString()));
        }
        abstractBuffer.writeAll(testRecords, 1000);

        // When
        abstractBuffer.read(1000);

        // Then
        final List<Measurement> recordsReadMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_READ).toString());
        final List<Measurement> recordsInFlightMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_INFLIGHT).toString());
        final List<Measurement> recordsProcessedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(MetricNames.RECORDS_PROCESSED).toString());
        final List<Measurement> readTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.READ_TIME_ELAPSED).toString());
        assertEquals(1, recordsReadMeasurements.size());
        assertEquals(5.0, recordsReadMeasurements.get(0).getValue(), 0);
        assertEquals(1, recordsInFlightMeasurements.size());
        assertEquals(5, abstractBuffer.getRecordsInFlight());
        final Measurement recordsInFlightMeasurement = recordsInFlightMeasurements.get(0);
        assertEquals(5.0, recordsInFlightMeasurement.getValue(), 0);
        assertEquals(1, recordsProcessedMeasurements.size());
        final Measurement recordsProcessedMeasurement = recordsProcessedMeasurements.get(0);
        assertEquals(0.0, recordsProcessedMeasurement.getValue(), 0);
        assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(readTimeMeasurements, Statistic.COUNT).getValue(), 0);
        assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(readTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.05,
                0.25));
    }

    @Test
    public void testCheckpointMetrics() throws Exception {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = new ArrayList<>();
        for(int i=0; i<5; i++) {
            testRecords.add(new Record<>(UUID.randomUUID().toString()));
        }
        abstractBuffer.writeAll(testRecords, 1000);
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = abstractBuffer.read(1000);
        final List<Measurement> checkpointTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.CHECKPOINT_TIME_ELAPSED).toString());
        assertEquals(0.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.COUNT).getValue(), 0);
        assertEquals(0.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.TOTAL_TIME).getValue(), 0);

        // When
        abstractBuffer.checkpoint(readResult.getValue());

        // Then
        assertEquals(0, abstractBuffer.getRecordsInFlight());
        final List<Measurement> recordsInFlightMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_INFLIGHT).toString());
        final List<Measurement> recordsProcessedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(MetricNames.RECORDS_PROCESSED).toString());
        final Measurement recordsInFlightMeasurement = recordsInFlightMeasurements.get(0);
        final Measurement recordsProcessedMeasurement = recordsProcessedMeasurements.get(0);
        assertEquals(0.0, recordsInFlightMeasurement.getValue(), 0);
        assertEquals(5.0, recordsProcessedMeasurement.getValue(), 0);
        assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.COUNT).getValue(), 0);
        assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(checkpointTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.0,
                0.001));
    }

    @Test
    public void testWriteBytes() throws TimeoutException {
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferTimeoutImpl(testPluginSetting);
        byte[] bytes = new byte[2];
        assertThrows(RuntimeException.class, () -> abstractBuffer.writeBytes(bytes, "", 10));
    }

    @Test
    public void testWriteTimeoutMetric() throws TimeoutException {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferTimeoutImpl(testPluginSetting);

        // When/Then
        assertThrows(TimeoutException.class, () -> abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000));

        final List<Measurement> timeoutMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.WRITE_TIMEOUTS).toString());
        assertEquals(1, timeoutMeasurements.size());
        assertEquals(1.0, timeoutMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testWriteRecordsWriteFailedMetric() throws TimeoutException {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferTimeoutImpl(testPluginSetting);

        // When/Then
        assertThrows(TimeoutException.class, () -> abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000));

        final List<Measurement> recordsWriteFailedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_WRITE_FAILED).toString());
        assertEquals(1, recordsWriteFailedMeasurements.size());
        assertEquals(1.0, recordsWriteFailedMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testWriteAllTimeoutMetric() throws TimeoutException {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferTimeoutImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = Arrays.asList(
                new Record<>(UUID.randomUUID().toString()), new Record<>(UUID.randomUUID().toString()));

        // When/Then
        assertThrows(TimeoutException.class, () -> abstractBuffer.writeAll(testRecords, 1000));

        final List<Measurement> timeoutMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.WRITE_TIMEOUTS).toString());
        assertEquals(1, timeoutMeasurements.size());
        assertEquals(1.0, timeoutMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testUpdateLatency() {
        final AbstractBuffer<Record<Event>> abstractBuffer = new AbstractBufferEventImpl(testPluginSetting);
        final Collection<Record<Event>> testRecords = Arrays.asList(
                new Record<>(JacksonEvent.fromMessage(TEST_MESSAGE)));
        abstractBuffer.updateLatency(testRecords);
        assertEquals(abstractBuffer.getLatencyTimer().count(), 1);
        
    }

    @Test
    public void testWriteAllRecordsWriteFailedMetric() {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferRuntimeExceptionImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = Arrays.asList(
                new Record<>(UUID.randomUUID().toString()), new Record<>(UUID.randomUUID().toString()));

        // When/Then
        assertThrows(RuntimeException.class, () -> abstractBuffer.writeAll(testRecords, 1000));

        final List<Measurement> recordsWriteFailedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(BUFFER_NAME).add(MetricNames.RECORDS_WRITE_FAILED).toString());
        assertEquals(1, recordsWriteFailedMeasurements.size());
        assertEquals(2.0, recordsWriteFailedMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testWriteAllSizeOverflowException() {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferSizeOverflowImpl(testPluginSetting);
        final Collection<Record<String>> testRecords = Arrays.asList(
                new Record<>(UUID.randomUUID().toString()), new Record<>(UUID.randomUUID().toString()));

        // When/Then
        assertThrows(SizeOverflowException.class, () -> abstractBuffer.writeAll(testRecords, 1000));
    }

    @Test
    public void testWriteRuntimeException() {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferNpeImpl(BUFFER_NAME, PIPELINE_NAME);

        // When/Then
        assertThrows(NullPointerException.class, () -> abstractBuffer.write(new Record<>(UUID.randomUUID().toString()), 1000));
    }

    @Test
    public void testWriteAllRuntimeException() {
        // Given
        final AbstractBuffer<Record<String>> abstractBuffer = new AbstractBufferNpeImpl(BUFFER_NAME, PIPELINE_NAME);
        final Collection<Record<String>> testRecords = Arrays.asList(
                new Record<>(UUID.randomUUID().toString()), new Record<>(UUID.randomUUID().toString()));

        // When/Then
        assertThrows(NullPointerException.class, () -> abstractBuffer.writeAll(testRecords, 1000));
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
        public void doWriteAll(final Collection<Record<String>> records, final int timeoutInMillis) throws Exception {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
            queue.addAll(records);
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

        @Override
        public void doWriteAll(Collection<Record<String>> records, int timeoutInMillis) throws TimeoutException {
            throw new TimeoutException();
        }
    }

    public static class AbstractBufferRuntimeExceptionImpl extends AbstractBufferImpl {
        public AbstractBufferRuntimeExceptionImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doWrite(Record<String> record, int timeoutInMillis) {
            throw new RuntimeException();
        }

        @Override
        public void doWriteAll(Collection<Record<String>> records, int timeoutInMillis) {
            throw new RuntimeException();
        }
    }

    public static class AbstractBufferSizeOverflowImpl extends AbstractBufferImpl {
        public AbstractBufferSizeOverflowImpl(final PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doWriteAll(Collection<Record<String>> records, int timeoutInMillis) throws SizeOverflowException {
            throw new SizeOverflowException("test error message");
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

        @Override
        public void doWriteAll(Collection<Record<String>> records, int timeoutInMillis) throws Exception {
            throw new NullPointerException();
        }
    }

    public static class AbstractBufferEventImpl extends AbstractBuffer<Record<Event>> {
        public AbstractBufferEventImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public void doWrite(Record<Event> record, int timeoutInMillis) throws TimeoutException {
        }

        @Override
        public void doWriteAll(Collection<Record<Event>> records, int timeoutInMillis) throws Exception {
        }

        @Override
        public void doCheckpoint(final CheckpointState checkpointState) {

        }

        @Override
        public Map.Entry<Collection<Record<Event>>, CheckpointState> doRead(int timeoutInMillis) {
            return null;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    }
}
