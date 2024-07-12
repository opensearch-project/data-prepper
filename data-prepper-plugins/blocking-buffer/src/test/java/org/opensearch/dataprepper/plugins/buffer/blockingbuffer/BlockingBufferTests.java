/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.blockingbuffer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlockingBufferTests {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String ATTRIBUTE_BUFFER_SIZE = "buffer_size";
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final String PLUGIN_NAME = "BlockingBuffer";
    private static final int TEST_BATCH_SIZE = 3;
    private static final int TEST_BUFFER_SIZE = 13;
    private static final int TEST_WRITE_TIMEOUT = 10;
    private static final int TEST_BATCH_READ_TIMEOUT = 500;
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @BeforeEach
    public void setup() {
        Metrics.globalRegistry.getRegistries().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    @Test
    public void testCreationUsingPluginSetting() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingNullPluginSetting() {
        try {
            new BlockingBuffer<Record<String>>((PluginSetting) null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PluginSetting cannot be null")));
        }
    }

    @Test
    public void testCreationUsingDefaultPluginSettings() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(
                BlockingBuffer.getDefaultPluginSettings());
        assertThat(blockingBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingValues() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
    }

    @Test
    public void testInsertNull() throws TimeoutException {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        assertThrows(NullPointerException.class, () -> blockingBuffer.write(null, TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testWriteAllSizeOverflow() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        final Collection<Record<String>> testRecords = generateBatchRecords(TEST_BUFFER_SIZE + 1);
        assertThrows(SizeOverflowException.class, () -> blockingBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT));
        verifyBufferUsageMetric(0);
    }

    @Test
    public void testNoEmptySpaceWriteOnly() throws TimeoutException {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(1, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        blockingBuffer.write(new Record<>("FILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);
        assertThrows(TimeoutException.class, () -> blockingBuffer.write(new Record<>("TIMEOUT"), TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testNoAvailSpaceWriteAllOnly() throws Exception {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(2, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        final Collection<Record<String>> testRecords = generateBatchRecords(2);
        blockingBuffer.write(new Record<>("FILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);
        assertThrows(TimeoutException.class, () -> blockingBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testNoEmptySpaceAfterUncheckedRead() throws TimeoutException {
        // Given
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(1, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        blockingBuffer.write(new Record<>("FILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);

        // When
        blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);

        // Then
        final Record<String> timeoutRecord = new Record<>("TIMEOUT");
        assertThrows(TimeoutException.class, () -> blockingBuffer.write(timeoutRecord, TEST_WRITE_TIMEOUT));
        assertThrows(
                TimeoutException.class, () -> blockingBuffer.writeAll(Collections.singletonList(timeoutRecord), TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testWriteIntoEmptySpaceAfterCheckedRead() throws TimeoutException {
        // Given
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(1, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        blockingBuffer.write(new Record<>("FILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);

        // When
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        blockingBuffer.checkpoint(readResult.getValue());

        // Then
        blockingBuffer.write(new Record<>("REFILL_THE_BUFFER"), TEST_WRITE_TIMEOUT);
        final Map.Entry<Collection<Record<String>>, CheckpointState> readCheckResult = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertEquals(1, readCheckResult.getKey().size());
    }

    @Test
    public void testWriteAllIntoEmptySpaceAfterCheckedRead() throws Exception {
        // Given
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(2, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        final Collection<Record<String>> testRecords = generateBatchRecords(2);
        blockingBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);
        verifyBufferUsageMetric(100.0);

        // When
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        blockingBuffer.checkpoint(readResult.getValue());
        verifyBufferUsageMetric(0.0);

        // Then
        blockingBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);
        final Map.Entry<Collection<Record<String>>, CheckpointState> readCheckResult = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertEquals(2, readCheckResult.getKey().size());
    }

    @Test
    public void testReadEmptyBuffer() {
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(TEST_BUFFER_SIZE, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);
        assertThat(blockingBuffer, notNullValue());
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = blockingBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(readResult.getKey().size(), is(0));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, TEST_BATCH_READ_TIMEOUT})
    public void testBatchRead(final int readTimeout) throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(blockingBuffer, notNullValue());
        final int testSize = 5;
        for (int i = 0; i < testSize; i++) {
            Record<String> record = new Record<>("TEST" + i);
            blockingBuffer.write(record, TEST_WRITE_TIMEOUT);
        }
        verifyBufferUsageMetric(38.46153846153847);
        final Map.Entry<Collection<Record<String>>, CheckpointState> partialReadResult = blockingBuffer.read(readTimeout);
        final Collection<Record<String>> partialRecords = partialReadResult.getKey();
        final CheckpointState partialCheckpointState = partialReadResult.getValue();
        final int expectedBatchSize = (Integer) completePluginSetting.getAttributeFromSettings(ATTRIBUTE_BATCH_SIZE);
        assertThat(partialRecords.size(), is(expectedBatchSize));
        assertEquals(expectedBatchSize, partialCheckpointState.getNumRecordsToBeChecked());
        int i = 0;
        for (Record<String> record : partialRecords) {
            assertThat(record.getData(), equalTo("TEST" + i));
            i++;
        }
        verifyBufferUsageMetric(38.46153846153847);
        blockingBuffer.checkpoint(partialReadResult.getValue());
        verifyBufferUsageMetric(15.384615384615385);
        final Map.Entry<Collection<Record<String>>, CheckpointState> finalReadResult = blockingBuffer.read(readTimeout);
        final Collection<Record<String>> finalBatch = finalReadResult.getKey();
        final CheckpointState finalCheckpointState = finalReadResult.getValue();
        assertThat(finalBatch.size(), is(testSize - expectedBatchSize));
        assertEquals(testSize - expectedBatchSize, finalCheckpointState.getNumRecordsToBeChecked());
        for (Record<String> record : finalBatch) {
            assertThat(record.getData(), equalTo("TEST" + i));
            i++;
        }
        verifyBufferUsageMetric(15.384615384615385);
        blockingBuffer.checkpoint(finalReadResult.getValue());
        verifyBufferUsageMetric(0.0);
    }

    @Test
    public void testBufferIsEmpty() {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);

        assertTrue(blockingBuffer.isEmpty());
        verifyBufferUsageMetric(0);
    }

    @Test
    public void testBufferIsNotEmpty() throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(completePluginSetting);

        Record<String> record = new Record<>("TEST");
        blockingBuffer.write(record, TEST_WRITE_TIMEOUT);

        assertFalse(blockingBuffer.isEmpty());
        verifyBufferUsageMetric(7.6923076923076925);
    }

    @Test
    void testNonZeroBatchDelayReturnsAllRecords() throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> buffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(buffer, notNullValue());

        final Collection<Record<String>> testRecords = generateBatchRecords(1);
        buffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);

        final Collection<Record<String>> testRecords2 = generateBatchRecords(1);
        EXECUTOR.submit(() -> {
            try {
                Thread.sleep(TEST_BATCH_READ_TIMEOUT / 2);
                buffer.writeAll(testRecords2, TEST_WRITE_TIMEOUT);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(TEST_BATCH_READ_TIMEOUT);
        final Collection<Record<String>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        assertThat(records.size(), is(testRecords.size() + testRecords2.size()));
        assertThat(checkpointState.getNumRecordsToBeChecked(), is(testRecords.size() + testRecords2.size()));
    }

    @Test
    void testZeroBatchDelayReturnsAvailableRecords() throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForBlockingBuffer();
        final BlockingBuffer<Record<String>> buffer = new BlockingBuffer<>(completePluginSetting);
        assertThat(buffer, notNullValue());

        final Collection<Record<String>> testRecords = generateBatchRecords(1);
        buffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);

        final Collection<Record<String>> testRecords2 = generateBatchRecords(1);
        EXECUTOR.submit(() -> {
            try {
                Thread.sleep(1000);
                buffer.writeAll(testRecords2, TEST_WRITE_TIMEOUT);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(0);
        final Collection<Record<String>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        assertThat(records.size(), is(1));
        assertThat(checkpointState.getNumRecordsToBeChecked(), is(1));
    }

    @ParameterizedTest
    @ArgumentsSource(BufferValuesArgumentProvider.class)
    public void testBufferUsage(final int recordsInBuffer, final int bufferSize, final double expectedValue) throws Exception {

        final BlockingBuffer<Record<String>> blockingBuffer = new BlockingBuffer<>(bufferSize, TEST_BATCH_SIZE,
                TEST_PIPELINE_NAME);

        final Collection<Record<String>> testRecords = generateBatchRecords(recordsInBuffer);
        blockingBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);

        verifyBufferUsageMetric(expectedValue);
    }

    static class BufferValuesArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final Random random = new Random();
            final int randomInt = random.nextInt(1000);
            return Stream.of(
                    Arguments.of(0, randomInt + 1, 0.0),
                    Arguments.of(1, 100, 1.0),
                    Arguments.of(randomInt + 1, randomInt + 1, 100.0),
                    Arguments.of(randomInt, randomInt + 250, ((double) randomInt / (randomInt + 250)) * 100),
                    Arguments.of(6, 9, 66.66666666666666),
                    Arguments.of(531, 1000, 53.1),
                    Arguments.of(3, 29, 10.344827586206897)
            );
        }
    }

    private PluginSetting completePluginSettingForBlockingBuffer() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(ATTRIBUTE_BUFFER_SIZE, TEST_BUFFER_SIZE);
        settings.put(ATTRIBUTE_BATCH_SIZE, TEST_BATCH_SIZE);
        final PluginSetting testSettings = new PluginSetting(PLUGIN_NAME, settings);
        testSettings.setPipelineName(TEST_PIPELINE_NAME);
        return testSettings;
    }

    private Collection<Record<String>> generateBatchRecords(final int numRecords) {
        final Collection<Record<String>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            results.add(new Record<>(UUID.randomUUID().toString()));
        }
        return results;
    }

    public void verifyBufferUsageMetric(final double expectedBufferUsage) {
        final Gauge bufferUsage = Metrics.globalRegistry.get(new StringJoiner(MetricNames.DELIMITER)
                        .add(TEST_PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("bufferUsage").toString()).gauge();

        assertThat(bufferUsage, is(notNullValue()));
        assertThat(bufferUsage.value(), is(expectedBufferUsage));
    }
}
