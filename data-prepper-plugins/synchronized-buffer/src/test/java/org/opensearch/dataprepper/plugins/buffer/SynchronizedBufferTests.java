/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

@ExtendWith(MockitoExtension.class)
public class SynchronizedBufferTests {
    private static final Logger LOG = LoggerFactory.getLogger(SynchronizedBufferTests.class);
    private static final String MOCK_PIPELINE_NAME = "mock-pipeline";
    private static final int WRITE_TIMEOUT = 1000;
    private static final int READ_TIMEOUT = 500;
    private static final String SINGLE_RECORD_DATA_FORMAT = "{\"message\":\"test\"}";
    private static final String BATCH_RECORDS_DATA_FORMAT = "{\"message\":\"test-%d\"}";
    private static final int DEFAULT_BATCH_SIZE = 2;
    private static final int CUSTOM_BATCH_SIZE = 5;

    @Mock
    PipelineDescription pipelineDescription;

    @BeforeEach
    public void setup() {
        new ArrayList<>(Metrics.globalRegistry.getRegistries())
                .forEach(Metrics.globalRegistry::remove);
        new ArrayList<>(Metrics.globalRegistry.getMeters())
                .forEach(Metrics.globalRegistry::remove);
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    @Nested
    class WriteTests {
        @Test
        public void testSingleWriteAndReadReturnsCorrectRecord() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            // Use helper method for async write
            CompletableFuture<Void> future = writeRecordAsync(buffer, SINGLE_RECORD_DATA_FORMAT, WRITE_TIMEOUT);

            // Give time for write to be queued
            Thread.sleep(100);

            Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> readRecords = readResult.getKey();
            assertEquals(1, readRecords.size());
            assertEquals(SINGLE_RECORD_DATA_FORMAT, readRecords.iterator().next().getData());

            // Checkpoint to signal completion
            buffer.checkpoint(readResult.getValue());

            // Verify buffer is now empty
            assertTrue(buffer.isEmpty());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        @Test
        public void testMultipleWriteAndReadReturnsCorrectRecords() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            // Write two records using helper methods
            CompletableFuture<Void> future1 = writeRecordAsync(buffer, SINGLE_RECORD_DATA_FORMAT, WRITE_TIMEOUT);
            CompletableFuture<Void> future2 = writeRecordAsync(buffer, SINGLE_RECORD_DATA_FORMAT, WRITE_TIMEOUT);

            // Give time for writes to be queued
            Thread.sleep(100);

            // Read records (should get up to batch size)
            Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> readRecords = readResult.getKey();

            // Should read up to batch size (DEFAULT_BATCH_SIZE)
            assertEquals(DEFAULT_BATCH_SIZE, readRecords.size());

            // Checkpoint to signal completion
            buffer.checkpoint(readResult.getValue());

            // Wait for futures to complete
            CompletableFuture.allOf(future1, future2).get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        @Test
        public void testWriteAllAndReadReturnsAllRecords() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            List<String> dataList = IntStream.range(0, 10)
                    .mapToObj(i -> String.format(BATCH_RECORDS_DATA_FORMAT, i))
                    .collect(Collectors.toList());

            // Write records using helper method
            CompletableFuture<Void> future = writeRecordsAsync(buffer, dataList, WRITE_TIMEOUT);

            // Give time for writes to be queued
            Thread.sleep(100);

            // Read all records (may require multiple reads due to batch size)
            List<Record<String>> allReadRecords = new ArrayList<>();
            while (!buffer.isEmpty()) {
                Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
                Collection<Record<String>> readRecords = readResult.getKey();
                allReadRecords.addAll(readRecords);

                // Checkpoint to signal completion
                buffer.checkpoint(readResult.getValue());
            }

            // Ensure that all write records were read
            assertEquals(dataList.size(), allReadRecords.size());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        @Test
        public void testWriteNullRecordThrowsException() {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            Exception writeException = assertThrows(NullPointerException.class, () -> {
                buffer.write(null, WRITE_TIMEOUT);
            });

            Exception writeAllException = assertThrows(NullPointerException.class, () -> {
                buffer.writeAll(null, WRITE_TIMEOUT);
            });

            assertEquals("The write record cannot be null", writeException.getMessage());
            assertEquals("The write records cannot be null", writeAllException.getMessage());
        }

        @Test
        public void testWriteEmptyRecordDoesNotThrowException() {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();
            assertDoesNotThrow(() -> {
                CompletableFuture<Void> writeFuture = writeRecordAsync(buffer, null, WRITE_TIMEOUT);
                // Give time for write to be queued
                Thread.sleep(100);
                // Read and checkpoint to complete the write
                Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
                buffer.checkpoint(readResult.getValue());

                writeFuture.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
            });

            assertDoesNotThrow(() -> {
                CompletableFuture<Void> writeAllFuture = writeRecordsAsync(buffer, null, WRITE_TIMEOUT);
            });
        }

        @Test
        public void testWriteTimeout() {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            // Write a record but don't read/checkpoint it
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> future = executor.submit(() -> {
                try {
                    buffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), 100);
                    fail("Expected TimeoutException");
                } catch (TimeoutException e) {
                    LOG.info("Successfully encountered timeout exception");
                }
            });

            // Wait for the future to complete
            try {
                future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                fail("Unexpected exception: " + e.getMessage());
            } finally {
                executor.shutdown();
            }
        }
    }

    @Nested
    class ReadTests {
        @Test
        public void testReadFromNonEmptyBufferReturnsCorrectRecords() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            CompletableFuture<Void> future = writeRecordAsync(buffer, SINGLE_RECORD_DATA_FORMAT, WRITE_TIMEOUT);

            // Give time for write to be queued
            Thread.sleep(100);

            // First read should return the record
            Map.Entry<Collection<Record<String>>, CheckpointState> initialReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> initialReadRecords = initialReadResult.getKey();
            assertEquals(1, initialReadRecords.size());
            assertEquals(SINGLE_RECORD_DATA_FORMAT, initialReadRecords.iterator().next().getData());

            // Checkpoint to signal completion
            buffer.checkpoint(initialReadResult.getValue());

            // Second read should return empty collection
            Map.Entry<Collection<Record<String>>, CheckpointState> secondReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> secondAttemptToReadRecords = secondReadResult.getKey();
            assertEquals(0, secondAttemptToReadRecords.size());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        @Test
        public void testReadFromEmptyBufferReturnsNoRecords() {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
            assertTrue(readResult.getKey().isEmpty());
        }

        @Test
        public void testReadBatchSizeRespected() throws Exception {
            // Create buffer with custom batch size
            SynchronizedBufferConfig config = new SynchronizedBufferConfig();
            setField(SynchronizedBufferConfig.class, config, "batchSize", CUSTOM_BATCH_SIZE);
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest(config);

            // Write more records than batch size
            List<String> dataList = IntStream.range(0, CUSTOM_BATCH_SIZE + 3)
                    .mapToObj(i -> String.format(BATCH_RECORDS_DATA_FORMAT, i))
                    .collect(Collectors.toList());

            CompletableFuture<Void> future = writeRecordsAsync(buffer, dataList, WRITE_TIMEOUT);

            // Give time for writes to be queued
            Thread.sleep(100);

            // First read should return exactly batch size records
            Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> readRecords = readResult.getKey();
            assertEquals(CUSTOM_BATCH_SIZE, readRecords.size());

            // Checkpoint to signal completion
            buffer.checkpoint(readResult.getValue());

            // Second read should return remaining records
            Map.Entry<Collection<Record<String>>, CheckpointState> secondReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> secondReadRecords = secondReadResult.getKey();
            assertEquals(3, secondReadRecords.size());

            // Checkpoint to signal completion
            buffer.checkpoint(secondReadResult.getValue());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    @Nested
    class EmptyBufferTests {
        @Test
        public void testIsEmptyReturnsTrueWhenBufferIsEmpty() {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();
            assertTrue(buffer.isEmpty());
        }

        @Test
        public void testIsEmptyReturnsFalseWhenBufferIsNotEmpty() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            // Write using helper method
            CompletableFuture<Void> future = writeRecordAsync(buffer, SINGLE_RECORD_DATA_FORMAT, WRITE_TIMEOUT);

            // Give time for write to be queued
            Thread.sleep(100);

            assertFalse(buffer.isEmpty());

            // Read and checkpoint to complete the write
            Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
            buffer.checkpoint(readResult.getValue());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    @Nested
    class ConcurrencyTests {
        @Test
        public void testMultipleWritersAndReaders() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();
            int numWriters = 5;
            int recordsPerWriter = 10;
            int totalRecords = numWriters * recordsPerWriter;

            // Create multiple writer threads
            ExecutorService writerExecutor = Executors.newFixedThreadPool(numWriters);
            List<Future<?>> writerFutures = new ArrayList<>();

            for (int i = 0; i < numWriters; i++) {
                final int writerIndex = i;
                writerFutures.add(writerExecutor.submit(() -> {
                    try {
                        for (int j = 0; j < recordsPerWriter; j++) {
                            String data = String.format("{\"writer\":%d,\"record\":%d}", writerIndex, j);
                            buffer.write(generateRecord(data), WRITE_TIMEOUT);
                        }
                    } catch (Exception e) {
                        fail("Exception in writer thread: " + e.getMessage());
                    }
                }));
            }

            // Create reader thread
            ExecutorService readerExecutor = Executors.newSingleThreadExecutor();
            Future<Integer> readerFuture = readerExecutor.submit(() -> {
                int recordsRead = 0;
                try {
                    while (recordsRead < totalRecords) {
                        Map.Entry<Collection<Record<String>>, CheckpointState> readResult = buffer.read(READ_TIMEOUT);
                        Collection<Record<String>> records = readResult.getKey();
                        if (!records.isEmpty()) {
                            recordsRead += records.size();
                            buffer.checkpoint(readResult.getValue());
                        } else {
                            Thread.sleep(10); // Small delay if no records
                        }
                    }
                } catch (Exception e) {
                    fail("Exception in reader thread: " + e.getMessage());
                }
                return recordsRead;
            });

            // Wait for reader to finish
            int totalReadRecords = readerFuture.get(WRITE_TIMEOUT * 2, TimeUnit.MILLISECONDS);
            assertEquals(totalRecords, totalReadRecords);

            // Shutdown executors
            writerExecutor.shutdown();
            readerExecutor.shutdown();
            assertTrue(writerExecutor.awaitTermination(WRITE_TIMEOUT, TimeUnit.MILLISECONDS));
            assertTrue(readerExecutor.awaitTermination(WRITE_TIMEOUT, TimeUnit.MILLISECONDS));

            // Verify all writer futures completed without exceptions
            for (Future<?> future : writerFutures) {
                assertDoesNotThrow(() -> future.get(100, TimeUnit.MILLISECONDS));
            }
        }

        @Test
        public void testPartialBatchProcessing() throws Exception {
            SynchronizedBuffer<Record<String>> buffer = createObjectUnderTest();

            // Write 5 records
            List<String> dataList = IntStream.range(0, 5)
                    .mapToObj(i -> String.format(BATCH_RECORDS_DATA_FORMAT, i))
                    .collect(Collectors.toList());

            // Write records using helper method
            CompletableFuture<Void> future = writeRecordsAsync(buffer, dataList, WRITE_TIMEOUT);

            // Give time for writes to be queued
            Thread.sleep(100);

            // Read first batch (should be DEFAULT_BATCH_SIZE)
            Map.Entry<Collection<Record<String>>, CheckpointState> firstReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> firstReadRecords = firstReadResult.getKey();
            assertEquals(DEFAULT_BATCH_SIZE, firstReadRecords.size());

            // Checkpoint to signal completion of first batch
            buffer.checkpoint(firstReadResult.getValue());

            // Read second batch (should be remaining records)
            Map.Entry<Collection<Record<String>>, CheckpointState> secondReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> secondReadRecords = secondReadResult.getKey();
            assertEquals(2, secondReadRecords.size());

            // Checkpoint to signal completion of second batch
            buffer.checkpoint(secondReadResult.getValue());

            Map.Entry<Collection<Record<String>>, CheckpointState> thirdReadResult = buffer.read(READ_TIMEOUT);
            Collection<Record<String>> thirdReadRecords = thirdReadResult.getKey();
            assertEquals(1, thirdReadRecords.size());

            // Checkpoint to signal completion of third batch
            buffer.checkpoint(thirdReadResult.getValue());

            // Wait for future to complete
            future.get(WRITE_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    /*-------------------------Private Helper Methods---------------------------*/
    private <T> CompletableFuture<Void> writeRecordAsync(SynchronizedBuffer<Record<T>> buffer, T data, int timeout) {
        return CompletableFuture.runAsync(() -> {
            try {
                buffer.write(generateRecord(data), timeout);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    private <T> CompletableFuture<Void> writeRecordsAsync(SynchronizedBuffer<Record<T>> buffer, Collection<T> data, int timeout) {
        return CompletableFuture.runAsync(() -> {
            try {
                Collection<Record<T>> records = generateRecords(data);
                buffer.writeAll(records, timeout);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    private <T> Record<T> generateRecord(final T data) {
        return new Record<>(data);
    }

    private <T> Collection<Record<T>> generateRecords(Collection<T> data) {
        Collection<Record<T>> records = new ArrayList<>();
        for (T recordData : data) {
            Record<T> record = new Record<>(recordData);
            records.add(record);
        }
        return records;
    }

    private <T> SynchronizedBuffer<Record<T>> createObjectUnderTest() {
        SynchronizedBufferConfig config = new SynchronizedBufferConfig();
        return createObjectUnderTest(config);
    }

    private <T> SynchronizedBuffer<Record<T>> createObjectUnderTest(SynchronizedBufferConfig config) {
        when(pipelineDescription.getPipelineName()).thenReturn(MOCK_PIPELINE_NAME);
        return new SynchronizedBuffer<>(config, pipelineDescription);
    }
}
