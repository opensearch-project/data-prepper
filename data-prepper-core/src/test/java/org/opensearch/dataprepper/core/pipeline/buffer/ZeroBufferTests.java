package org.opensearch.dataprepper.core.pipeline.buffer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.pipeline.PipelineRunner;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ZeroBufferTests {
    private static final Logger LOG = LoggerFactory.getLogger(ZeroBufferTests.class);
    private static final String MOCK_PIPELINE_NAME = "mock-pipeline";
    private static final int WRITE_TIMEOUT = 100;
    private static final int READ_TIMEOUT = 500;
    private static final String SINGLE_RECORD_DATA_FORMAT = "{\"message\":\"test\"}";
    private static final String BATCH_RECORDS_DATA_FORMAT = "{\"message\":\"test-%d\"}";

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
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);

            Collection<Record<String>> readRecords = zeroBuffer.read(READ_TIMEOUT).getKey();
            assertEquals(1, readRecords.size());

            assertEquals(SINGLE_RECORD_DATA_FORMAT, readRecords.iterator().next().getData());

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);
            readRecords = zeroBuffer.read(READ_TIMEOUT).getKey();
            assertEquals(1, readRecords.size());
        }

        @Test
        public void testMultipleWriteAndReadReturnsCorrectRecord() throws Exception {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);
            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);

            Collection<Record<String>> readRecords = zeroBuffer.read(READ_TIMEOUT).getKey();
            assertEquals(2, readRecords.size());
            assertEquals(SINGLE_RECORD_DATA_FORMAT, readRecords.iterator().next().getData());
            assertEquals(SINGLE_RECORD_DATA_FORMAT, readRecords.iterator().next().getData());
        }

        @Test
        public void testWriteAllAndReadReturnsAllRecords() throws Exception {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            Collection<Record<String>> writeRecords = generateRecords(IntStream.range(0, 10)
                    .mapToObj(i -> String.format(BATCH_RECORDS_DATA_FORMAT, i))
                    .collect(Collectors.toList()));
            zeroBuffer.writeAll(writeRecords, WRITE_TIMEOUT);

            Map.Entry<Collection<Record<String>>, CheckpointState> readRecordsMap = zeroBuffer.read(READ_TIMEOUT);
            Collection<Record<String>> readRecords = readRecordsMap.getKey();
            for (Record<String> record : readRecords) {
                LOG.debug(record.getData());
            }

            // Ensure that the write records are the same as the read records
            assertEquals(writeRecords.size(), readRecords.size());
        }

        @Test
        public void testWriteNullRecordThrowsException() {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            Exception writeException = assertThrows(NullPointerException.class, () -> {
                zeroBuffer.write(null, WRITE_TIMEOUT);
            });

            Exception writeAllException = assertThrows(NullPointerException.class, () -> {
                zeroBuffer.writeAll(null, WRITE_TIMEOUT);
            });

            assertEquals("The write record cannot be null", writeException.getMessage());
            assertEquals("The write records cannot be null", writeAllException.getMessage());
        }

        @Test
        public void testWriteEmptyRecordDoesNotThrowException() {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            Record<String> emptyRecord = generateRecord(null);
            Collection<Record<String>> emptyRecordCollection = generateRecords(new ArrayList<>());

            assertDoesNotThrow(() -> zeroBuffer.write(emptyRecord, WRITE_TIMEOUT));
            assertDoesNotThrow(() -> zeroBuffer.writeAll(emptyRecordCollection, WRITE_TIMEOUT));
        }

        @Test
        public void testThreadReadAndWriteIsolation() throws Exception {
            final ZeroBuffer<Record<String>> zeroBuffer = initializeZeroBufferWithPipelineName();

            Thread workerThread = new Thread(() -> {
                try {
                    PipelineRunner pipelineRunnerMock = mock(PipelineRunner.class);
                    zeroBuffer.setPipelineRunner(pipelineRunnerMock);
                    doNothing().when(pipelineRunnerMock).runAllProcessorsAndPublishToSinks();
                    zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);
                } catch (TimeoutException e) {
                    fail("Timeout exception occurred");
                }
            });
            workerThread.start();
            workerThread.join();

            // Ensure that main thread does not share the same records store as the worker thread
            assertEquals(0, zeroBuffer.read(READ_TIMEOUT).getKey().size());
            assertTrue(zeroBuffer.isEmpty());
        }

        @Test
        public void testWriteAndWriteAllReturnsCorrectRecords() throws Exception {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);
            zeroBuffer.writeAll(generateRecords(IntStream.range(0, 10)
                    .mapToObj(i -> String.format(BATCH_RECORDS_DATA_FORMAT, i))
                    .collect(Collectors.toList())), WRITE_TIMEOUT);

            Collection<Record<String>> readRecords = zeroBuffer.read(READ_TIMEOUT).getKey();
            for (Record<String> record : readRecords) {
                LOG.debug(record.getData());
            }
            assertEquals(11, readRecords.size());
        }
    }

    @Nested
    class ReadTests {
        @Test
        public void testReadFromNonEmptyBufferReturnsCorrectRecords() throws Exception {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);

            Collection<Record<String>> initialReadRecords = zeroBuffer.read(READ_TIMEOUT).getKey();
            Collection<Record<String>> secondAttemptToReadRecords = zeroBuffer.read(READ_TIMEOUT).getKey();

            assertEquals(1, initialReadRecords.size());
            assertEquals(SINGLE_RECORD_DATA_FORMAT, initialReadRecords.iterator().next().getData());

            assertEquals(0, secondAttemptToReadRecords.size());
        }

        @Test
        public void testReadFromEmptyBufferReturnsNoRecords() {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            Map.Entry<Collection<Record<String>>, CheckpointState> readRecordsMap = zeroBuffer.read(READ_TIMEOUT);
            assertTrue(readRecordsMap.getKey().isEmpty());
        }
    }

    @Nested
    class EmptyBufferTests {
        @Test
        public void testIsEmptyReturnsTrueWhenBufferIsEmpty() {
            ZeroBuffer<Record<String>> zeroBuffer = initializeZeroBufferWithPipelineName();
            assertTrue(zeroBuffer.isEmpty());
        }

        @Test
        public void testIsEmptyReturnsFalseWhenBufferIsNotEmpty() throws Exception {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();

            zeroBuffer.write(generateRecord(SINGLE_RECORD_DATA_FORMAT), WRITE_TIMEOUT);

            assertFalse(zeroBuffer.isEmpty());
        }
    }

    @Nested
    class CommonTests {
        @Test
        public void testCreateZeroBufferWithPipelineName() {
            ZeroBuffer<Record<String>> zeroBuffer = initializeZeroBufferWithPipelineName();
            assertEquals(MOCK_PIPELINE_NAME, zeroBuffer.pipelineName);
        }

        @Test
        public void testCheckpointDoesNotThrowException() {
            ZeroBuffer<Record<String>> zeroBuffer = setupAndInitializeZeroBuffer();
            assertDoesNotThrow(() -> zeroBuffer.checkpoint(null));
            assertDoesNotThrow(() -> zeroBuffer.checkpoint(new CheckpointState(0)));
        }
    }

    /*-------------------------Private Helper Methods---------------------------*/
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

    private <T> ZeroBuffer<Record<T>> setupAndInitializeZeroBuffer() {
        ZeroBuffer<Record<T>> zeroBuffer = initializeZeroBufferWithPipelineName();
        PipelineRunner pipelineRunnerMock = mock(PipelineRunner.class);
        zeroBuffer.setPipelineRunner(pipelineRunnerMock);
        doNothing().when(pipelineRunnerMock).runAllProcessorsAndPublishToSinks();
        return zeroBuffer;
    }

    private <T> ZeroBuffer<Record<T>> initializeZeroBufferWithPipelineName() {
        PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(MOCK_PIPELINE_NAME);
        return new ZeroBuffer<>(pipelineDescription);
    }
}
