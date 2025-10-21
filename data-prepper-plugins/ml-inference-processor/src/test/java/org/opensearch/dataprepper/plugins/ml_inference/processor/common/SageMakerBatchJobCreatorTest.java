/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.client.S3ClientFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.MLBatchJobFailedDlqData;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig.DEFAULT_RETRY_WINDOW;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.NUMBER_OF_FAILED_BATCH_JOBS_CREATION;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.NUMBER_OF_RECORDS_FAILED_IN_BATCH_JOB;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.NUMBER_OF_RECORDS_SUCCEEDED_IN_BATCH_JOB;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION;

public class SageMakerBatchJobCreatorTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Mock
    private S3Client s3Client;

    @Mock
    private DlqPushHandler dlqPushHandler;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private Counter recordsSuccessCounter;

    @Mock
    private Counter recordsFailedCounter;

    private SageMakerBatchJobCreator sageMakerBatchJobCreator;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private Counter counter;
    private ConcurrentLinkedQueue<Record<Event>> batch_records;
    private ConcurrentLinkedQueue<Record<Event>> processedBatchRecords;
    private static final int NUM_THREADS = 5;
    private static final int BATCH_SIZE = 100;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/sagemaker/output");
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        final EventKey sourceKey = eventKeyFactory.createEventKey("key");
        when(mlProcessorConfig.getInputKey()).thenReturn(sourceKey);
        when(mlProcessorConfig.getRetryTimeWindow()).thenReturn(DEFAULT_RETRY_WINDOW);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        counter = mock(Counter.class);
        when(pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_FAILED_IN_BATCH_JOB)).thenReturn(recordsFailedCounter);
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_SUCCEEDED_IN_BATCH_JOB)).thenReturn(recordsSuccessCounter);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline_sagemaker");
        when(pluginSetting.getName()).thenReturn("pipeline_sagemaker");

        try (MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {
            mockedS3ClientFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier)).thenReturn(s3Client);

            // Create a spy of the real object
            sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler));

            // Get access to the private queues for testing
            Field batchRecordsField = SageMakerBatchJobCreator.class.getDeclaredField("batch_records");
            batchRecordsField.setAccessible(true);
            batch_records = (ConcurrentLinkedQueue<Record<Event>>) batchRecordsField.get(sageMakerBatchJobCreator);

            Field processedBatchRecordsField = SageMakerBatchJobCreator.class.getDeclaredField("processedBatchRecords");
            processedBatchRecordsField.setAccessible(true);
            processedBatchRecords = (ConcurrentLinkedQueue<Record<Event>>) processedBatchRecordsField.get(sageMakerBatchJobCreator);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testCreateMLBatchJob_AddsRecordsToBatch() {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Act
        sageMakerBatchJobCreator.createMLBatchJob(records, resultRecords);

        // Assert
        assertEquals(1, batch_records.size());
        assertEquals(record, batch_records.peek());
        assertEquals(0, resultRecords.size());
    }

    @Test
    void testAddProcessedBatchRecordsToResults() {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        processedBatchRecords.add(record);
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Act
        sageMakerBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);

        // Assert
        assertEquals(1, resultRecords.size());
        assertEquals(record, resultRecords.get(0));
        assertTrue(processedBatchRecords.isEmpty());
    }

    @Test
    void testCheckAndProcessBatch_ProcessesWhenMaxSizeReached() throws Exception {
        // Arrange - fill batch_records to max size
        for (int i = 0; i < 100; i++) {
            Event event = createMockEvent("test-bucket", "input" + i + ".jsonl");
            // Make sure the event's key is properly accessible
            batch_records.add(new Record<>(event));
        }

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class);
             MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {

            // Make RetryUtil actually execute the supplier
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(true, null, 1));

            mockedS3ClientFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier)).thenReturn(s3Client);
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act - trigger batch processing directly
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementSuccessCounter();
            verify(counter, times(1)).increment(); // Success counter
            assertEquals(100, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testCheckAndProcessBatch_ProcessesWhenTimeoutReached() throws Exception {
        // Arrange - add a few records and set lastUpdateTimestamp to be old
        Event event = createMockEvent("test-bucket", "input.jsonl");
        batch_records.add(new Record<>(event));

        // Set lastUpdateTimestamp to be old
        Field lastUpdateTimestampField = SageMakerBatchJobCreator.class.getDeclaredField("lastUpdateTimestamp");
        lastUpdateTimestampField.setAccessible(true);
        lastUpdateTimestampField.set(sageMakerBatchJobCreator, java.util.concurrent.atomic.AtomicLong.class.getDeclaredConstructor(long.class).newInstance(System.currentTimeMillis() - 70000)); // 70 seconds ago

        // Mock RetryUtil to return success
        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(true, null, 1));
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementSuccessCounter();
            verify(counter, times(1)).increment(); // Success counter
            assertEquals(1, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testHandleFailure_NonMLBatchJobException() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);
        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));
        when(event.toJsonString()).thenReturn("event");

        // Create a generic RuntimeException
        RuntimeException genericException = new RuntimeException("Generic error message");
        when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // RetryUtil returns false with a non-MLBatchJobException
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, genericException, 3));  // 3 attempts made

            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementFailureCounter();
            verify(counter).increment();

            // Verify DLQ handling
            ArgumentCaptor<List<DlqObject>> dlqCaptor = ArgumentCaptor.forClass(List.class);
            verify(dlqPushHandler).perform(dlqCaptor.capture());

            List<DlqObject> dlqObjects = dlqCaptor.getValue();
            assertEquals(1, dlqObjects.size());
            DlqObject dlqObject = dlqObjects.get(0);
            MLBatchJobFailedDlqData mlBatchJobFailedDlqData = (MLBatchJobFailedDlqData)dlqObject.getFailedData();
            assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, mlBatchJobFailedDlqData.getStatus());
            assertTrue(mlBatchJobFailedDlqData.getMessage().contains("Generic error message"));

            assertEquals(1, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testProcessRemainingBatch() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        batch_records.add(new Record<>(event));

        // Mock RetryUtil to return success
        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(true, null, 1));
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act
            callPrivateProcessRemainingBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementSuccessCounter();
            verify(counter, times(1)).increment(); // Success counter
            assertEquals(1, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testHandleFailure_S3UploadException() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {
            mockedS3ClientFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier))
                    .thenReturn(s3Client);

            // S3 throws exception during upload
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenThrow(new RuntimeException("S3 upload failed"));

            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementFailureCounter();
            verify(counter).increment();
            assertEquals(1, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testHandleFailure_EmptyInputCollection() throws Exception {
        // Arrange - create an empty batch
        List<Record<Event>> emptyBatch = new ArrayList<>();

        // Act - directly call processCurrentBatch with reflection
        Method processMethod = SageMakerBatchJobCreator.class.getDeclaredMethod("processCurrentBatch", List.class);
        processMethod.setAccessible(true);
        processMethod.invoke(sageMakerBatchJobCreator, emptyBatch);

        // Assert
        verify(sageMakerBatchJobCreator).incrementFailureCounter();
        verify(counter).increment();
    }

    @Test
    void testHandleFailure_RetryFailure() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // RetryUtil returns false indicating failure
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(500, "errorMessage") , 1));

            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(sageMakerBatchJobCreator).incrementFailureCounter();
            verify(counter).increment();
            assertEquals(1, processedBatchRecords.size());
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testIsReadyForShutdown_WhenBatchEmpty() {
        // Arrange
        batch_records.clear();

        // Act & Assert
        assertTrue(sageMakerBatchJobCreator.isReadyForShutdown());
    }

    @Test
    void testIsReadyForShutdown_WhenBatchNotEmpty() {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        batch_records.add(new Record<>(event));

        // Act & Assert
        assertFalse(sageMakerBatchJobCreator.isReadyForShutdown());
    }

    @Test
    void testHandleFailure() throws Exception {
        // Arrange
        for (int i = 0; i < 100; i++) {
            Event event = createMockEvent("test-bucket", "input" + i + ".jsonl");
            // Make sure the event's key is properly accessible
            batch_records.add(new Record<>(event));
        }
        // Mock RetryUtil to return failure
        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(500, "errorMessage") , 1));
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act - trigger batch processing with a batch that will fail
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(counter, times(1)).increment(); // Failure counter
            assertEquals(100, processedBatchRecords.size()); // Record should still be added but with failure tag
            assertTrue(batch_records.isEmpty());
        }
    }

    @Test
    void testConcurrentAddProcessedBatchRecordsToResults() throws InterruptedException {
        // Setup - First populate processedBatchRecords by processing a batch
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(NUM_THREADS);
        final AtomicInteger successfulThreads = new AtomicInteger(0);

        // Create initial batch to trigger processing
        List<Record<Event>> initialBatch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            Record<Event> mockRecord = mock(Record.class);
            Event mockEvent = mock(Event.class);
            JsonNode mockJsonNode = mock(JsonNode.class);

            when(mockRecord.getData()).thenReturn(mockEvent);
            when(mockEvent.getJsonNode()).thenReturn(mockJsonNode);
            when(mockJsonNode.get("bucket")).thenReturn(mock(JsonNode.class));
            when(mockJsonNode.get("bucket").asText()).thenReturn("test-bucket");
            when(mockJsonNode.get("key")).thenReturn(mock(JsonNode.class));
            when(mockJsonNode.get("key").asText()).thenReturn("test-key-" + i);

            initialBatch.add(mockRecord);
        }

        // Add records and process them to populate processedBatchRecords
        sageMakerBatchJobCreator.createMLBatchJob(initialBatch, new ArrayList<>());
        sageMakerBatchJobCreator.checkAndProcessBatch(); // This populates processedBatchRecords

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final List<List<Record<Event>>> threadResults = new ArrayList<>();

        // Create separate result lists for each thread to avoid ArrayList thread safety issues
        for (int i = 0; i < NUM_THREADS; i++) {
            threadResults.add(new ArrayList<>());
        }

        // Create concurrent tasks
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    // Each thread tries to add processed records to its own result list
                    List<Record<Event>> threadResultList = threadResults.get(threadIndex);
                    sageMakerBatchJobCreator.addProcessedBatchRecordsToResults(threadResultList);

                    successfulThreads.incrementAndGet();
                } catch (Exception e) {
                    fail("Thread " + threadIndex + " failed with exception: " + e.getMessage(), e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

        // Verify results
        assertEquals(NUM_THREADS, successfulThreads.get());

        // Verify that only ONE thread actually got the records (due to non-blocking lock)
        int threadsWithResults = 0;
        int totalRecords = 0;
        for (List<Record<Event>> resultList : threadResults) {
            if (!resultList.isEmpty()) {
                threadsWithResults++;
                totalRecords += resultList.size();
            }
        }

        assertEquals(1, threadsWithResults, "Only one thread should have acquired the lock and gotten results");
        assertEquals(BATCH_SIZE, totalRecords, "The successful thread should have gotten all processed records");
    }

    @Test
    void testConcurrentCheckAndProcessBatch() throws InterruptedException {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(NUM_THREADS);
        final AtomicInteger processAttemptsCount = new AtomicInteger(0);
        final AtomicInteger successfulProcessCount = new AtomicInteger(0);

        // Create batch that meets the size requirement for processing
        List<Record<Event>> initialBatch = new ArrayList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            Record<Event> mockRecord = mock(Record.class);
            Event mockEvent = mock(Event.class);
            JsonNode mockJsonNode = mock(JsonNode.class);

            when(mockRecord.getData()).thenReturn(mockEvent);
            when(mockEvent.getJsonNode()).thenReturn(mockJsonNode);
            when(mockJsonNode.get("bucket")).thenReturn(mock(JsonNode.class));
            when(mockJsonNode.get("bucket").asText()).thenReturn("test-bucket");
            when(mockJsonNode.get("key")).thenReturn(mock(JsonNode.class));
            when(mockJsonNode.get("key").asText()).thenReturn("test-key-" + i);

            initialBatch.add(mockRecord);
        }

        // Add the batch
        sageMakerBatchJobCreator.createMLBatchJob(initialBatch, new ArrayList<>());
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        // Submit concurrent processing tasks
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    processAttemptsCount.incrementAndGet();
                    sageMakerBatchJobCreator.checkAndProcessBatch();
                } catch (Exception e) {
                    fail("Thread " + threadIndex + " failed with exception: " + e.getMessage(), e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        assertTrue(completionLatch.await(10, TimeUnit.SECONDS));
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

        // Verify results
        assertEquals(NUM_THREADS, processAttemptsCount.get(), "All threads should have attempted processing");
        assertTrue(sageMakerBatchJobCreator.getBatch_records().isEmpty(), "Batch should be empty after processing");
    }

    @Test
    void testRetryLogic_ThrottlingFailure() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // Mock RetryUtil to simulate throttling failure
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "Rate limited"), 1));

            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            ConcurrentLinkedQueue<AbstractBatchJobCreator.RetryRecord> retryQueue = sageMakerBatchJobCreator.getRetryQueue();
            assertEquals(1, retryQueue.size());
            assertTrue(batch_records.isEmpty());
            assertTrue(processedBatchRecords.isEmpty());
            verify(counter, times(0)).increment();
        }
    }

    @Test
    void testRetryLogic_SuccessfulRetry() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // RetryUtil returns failure with 429 status
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "Rate limited"), 1))
                    .thenReturn(new RetryUtil.RetryResult(true, null, 1));
            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            ConcurrentLinkedQueue<AbstractBatchJobCreator.RetryRecord> retryQueue = sageMakerBatchJobCreator.getRetryQueue();
            assertEquals(1, retryQueue.size());
            assertTrue(batch_records.isEmpty());

            // Act - Second attempt
            sageMakerBatchJobCreator.checkAndProcessBatch();
            // Assert
            assertTrue(retryQueue.isEmpty());
            assertEquals(1, processedBatchRecords.size());
            verify(counter, times(1)).increment();
        }
    }

    @Test
    void testRetryLogic_FailedRetry() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // RetryUtil returns failure with 429 status
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "Rate limited"), 1))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(404, "timeout"), 1));
            // Act
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            ConcurrentLinkedQueue<AbstractBatchJobCreator.RetryRecord> retryQueue = sageMakerBatchJobCreator.getRetryQueue();
            assertEquals(1, retryQueue.size());
            assertTrue(batch_records.isEmpty());

            // Act - Second attempt
            when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);
            when(event.toJsonString()).thenReturn("event"); // Add this
            sageMakerBatchJobCreator.checkAndProcessBatch();
            // Assert
            assertTrue(retryQueue.isEmpty());
            assertEquals(1, processedBatchRecords.size());
            verify(dlqPushHandler).perform(any());
            verify(counter, times(1)).increment();
        }
    }

    @Test
    void testRetryLogic_ExpiredRetry() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        Record<Event> record = new Record<>(event);
        batch_records.add(record);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // Mock initial failure
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "Rate limited"), 1));

            // First attempt - adds to retry queue
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Get retry queue and simulate record expiration
            ConcurrentLinkedQueue<AbstractBatchJobCreator.RetryRecord> retryQueue = sageMakerBatchJobCreator.getRetryQueue();
            AbstractBatchJobCreator.RetryRecord retryRecord = retryQueue.peek();
            setPrivateField(retryRecord, "createdTime", System.currentTimeMillis() - (mlProcessorConfig.getRetryTimeWindow().toMillis() + 1));

            // Act - Process expired record
            when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);
            when(event.toJsonString()).thenReturn("event"); // Add this
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            assertTrue(retryQueue.isEmpty());
            assertEquals(1, processedBatchRecords.size());
            verify(dlqPushHandler).perform(any());
            verify(counter, times(1)).increment();
        }
    }

    @Test
    void testProcessBothRetryAndNewRecords() throws Exception {
        // Create record A for retry queue
        Event eventA = createMockEvent("test-bucket", "inputA.jsonl");
        Record<Event> recordA = new Record<>(eventA);

        // Create RetryRecord using reflection
        Object retryRecordA = createRetryRecord(recordA);
        sageMakerBatchJobCreator.getRetryQueue().add((AbstractBatchJobCreator.RetryRecord) retryRecordA);

        // Create record B for batch_records
        Event eventB = createMockEvent("test-bucket", "inputB.jsonl");
        Record<Event> recordB = new Record<>(eventB);
        List<Record<Event>> newRecords = Collections.singletonList(recordB);
        List<Record<Event>> resultRecords = new ArrayList<>();

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // Mock successful processing
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(true, null, 3));

            // Add record B to batch_records
            sageMakerBatchJobCreator.createMLBatchJob(newRecords, resultRecords);
            // Verify initial state
            assertEquals(1, sageMakerBatchJobCreator.getRetryQueue().size(), "Retry queue should have record A");
            assertEquals(1, sageMakerBatchJobCreator.getBatch_records().size(), "Batch records should have record B");

            sageMakerBatchJobCreator.checkAndProcessBatch();
            // Verify final state

            // Verify retry queue is empty
            assertTrue(sageMakerBatchJobCreator.getRetryQueue().isEmpty(),
                    "Retry queue should be empty after processing");

            // Verify batch_records is empty
            assertTrue(sageMakerBatchJobCreator.getBatch_records().isEmpty(),
                    "Batch records should be empty after processing");

            // Verify success counters
            verify(counter).increment();
            verify(recordsSuccessCounter).increment(2); // Both records A and B
        }
    }

    @Test
    void testProcessBothRetryAndNewRecords_WithThrottledAgain() throws Exception {
        // Create record A for retry queue
        Event eventA = createMockEvent("test-bucket", "inputA.jsonl");
        Record<Event> recordA = new Record<>(eventA);
        Object retryRecordA = createRetryRecord(recordA);
        sageMakerBatchJobCreator.getRetryQueue().add((AbstractBatchJobCreator.RetryRecord) retryRecordA);

        // Create record B for batch_records
        Event eventB = createMockEvent("test-bucket", "inputB.jsonl");
        Record<Event> recordB = new Record<>(eventB);
        List<Record<Event>> newRecords = Collections.singletonList(recordB);
        List<Record<Event>> resultRecords = new ArrayList<>();

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // Mock failure with throttling
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "Rate limited"), 1));

            sageMakerBatchJobCreator.createMLBatchJob(newRecords, resultRecords);

            // Verify initial state
            assertEquals(1, sageMakerBatchJobCreator.getRetryQueue().size(),
                    "Retry queue should have record A");
            assertEquals(1, sageMakerBatchJobCreator.getBatch_records().size(),
                    "Batch records should have record B");
            // Get retry count of record A before processing
            AbstractBatchJobCreator.RetryRecord retryRecordBefore =
                    sageMakerBatchJobCreator.getRetryQueue().peek();
            int initialRetryCount = getRetryCount(retryRecordBefore);

            // Process both records - should be throttled
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Verify retry queue contains both records
            assertEquals(2, sageMakerBatchJobCreator.getRetryQueue().size(),
                    "Retry queue should contain both records after throttling");
            assertEquals(0, sageMakerBatchJobCreator.getProcessedBatchRecords().size(),
                    "Retry queue should contain both records after throttling");
            // Verify batch_records is empty
            assertTrue(sageMakerBatchJobCreator.getBatch_records().isEmpty(),
                    "Batch records should be empty after processing");
            // Verify record A's retry count was incremented
            AbstractBatchJobCreator.RetryRecord updatedRetryRecordA =
                    sageMakerBatchJobCreator.getRetryQueue().stream()
                    .filter(r -> getRecordFromRetryRecord(r).getData().getJsonNode().get("key").asText().equals("inputA.jsonl"))
                    .findFirst()
                    .orElse(null);

            assertNotNull(updatedRetryRecordA, "Record A should still be in retry queue");
            assertEquals(initialRetryCount + 1, getRetryCount(updatedRetryRecordA),
                    "Retry count for record A should be incremented");

            // Verify record B was added to retry queue
            boolean recordBInRetryQueue = sageMakerBatchJobCreator.getRetryQueue().stream()
                    .anyMatch(r -> getRecordFromRetryRecord(r).getData().getJsonNode().get("key").asText().equals("inputB.jsonl"));
            assertTrue(recordBInRetryQueue, "Record B should be in retry queue");
        }
    }

    @Test
    void testProcessBothRetryAndNewRecords_WithFailure() throws Exception {
        // Create record A for retry queue
        Event eventA = createMockEvent("test-bucket", "inputA.jsonl");
        Record<Event> recordA = new Record<>(eventA);
        Object retryRecordA = createRetryRecord(recordA);
        sageMakerBatchJobCreator.getRetryQueue().add((AbstractBatchJobCreator.RetryRecord) retryRecordA);
        when(eventA.toJsonString()).thenReturn("eventA");

        // Create record B for batch_records
        Event eventB = createMockEvent("test-bucket", "inputB.jsonl");
        Record<Event> recordB = new Record<>(eventB);
        List<Record<Event>> newRecords = Collections.singletonList(recordB);
        List<Record<Event>> resultRecords = new ArrayList<>();
        when(eventB.toJsonString()).thenReturn("eventB");

        when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);
        // Capture DLQ objects
        ArgumentCaptor<List<DlqObject>> dlqObjectsCaptor = ArgumentCaptor.forClass(List.class);

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            // Mock 404 failure response
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(404, "Not Found"), 3));
            // Add record B to batch_records
            sageMakerBatchJobCreator.createMLBatchJob(newRecords, resultRecords);

            // Verify initial state
            assertEquals(1, sageMakerBatchJobCreator.getRetryQueue().size(),
                    "Retry queue should have record A");
            assertEquals(1, sageMakerBatchJobCreator.getBatch_records().size(),
                    "Batch records should have record B");

            // Process both records - should fail with 404
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Verify final state
            // Both records should be removed from their respective queues
            assertTrue(sageMakerBatchJobCreator.getRetryQueue().isEmpty(),
                    "Retry queue should be empty after failure");
            assertTrue(sageMakerBatchJobCreator.getBatch_records().isEmpty(),
                    "Batch records should be empty after failure");

            // Verify both records were added to processedBatchRecords with failure tags
            assertEquals(2, sageMakerBatchJobCreator.getProcessedBatchRecords().size(),
                    "Both records should be in result records");

            // Verify DLQ handling
            verify(dlqPushHandler).perform(dlqObjectsCaptor.capture());
            List<DlqObject> dlqObjects = dlqObjectsCaptor.getValue();
            assertEquals(2, dlqObjects.size(), "Both records should be sent to DLQ");

            // Verify DLQ objects content
            Set<String> dlqKeys = dlqObjects.stream()
                    .map(dlqObject -> ((MLBatchJobFailedDlqData) dlqObject.getFailedData()).getS3Key())
                    .collect(Collectors.toSet());
            assertTrue(dlqKeys.contains("inputA.jsonl"), "DLQ should contain record A");
            assertTrue(dlqKeys.contains("inputB.jsonl"), "DLQ should contain record B");
            // Verify all DLQ objects have correct status code
            dlqObjects.forEach(dlqObject -> {
                MLBatchJobFailedDlqData failedData = (MLBatchJobFailedDlqData) dlqObject.getFailedData();
                assertEquals(404, failedData.getStatus(), "DLQ object should have 404 status code");
                assertEquals("Failed to Create SageMaker batch job after 3 attempts: Not Found", failedData.getMessage(), "DLQ object should have correct error message");
            });
            // Verify counters
            verify(counter).increment(); // Failure counter
            verify(recordsFailedCounter).increment(2); // Both records failed
        }
    }

    @Test
    void generateJobName_ReturnsValidJobName() {
        // when
        String jobName = sageMakerBatchJobCreator.generateJobName();

        // then
        assertNotNull(jobName);
        assertThat(jobName.length(), lessThanOrEqualTo(63));
        assertThat(jobName, matchesPattern("^batch-job-\\d{17}-[a-f0-9-]{1,36}$"));
    }

    // Helper methods
    private Event createMockEvent(String bucket, String key) {
        Event event = mock(Event.class);
        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", bucket)
                .put("key", key));
        // Mock the event.get method which is used when inputKey is not null
        when(event.get(mlProcessorConfig.getInputKey(), String.class)).thenReturn(key);
        return event;
    }

    private void callPrivateProcessRemainingBatch() throws Exception {
        // Use reflection to call private method
        java.lang.reflect.Method method = SageMakerBatchJobCreator.class.getDeclaredMethod("processRemainingBatch");
        method.setAccessible(true);
        method.invoke(sageMakerBatchJobCreator);
    }

    private void setPrivateField(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set private field: " + fieldName, e);
        }
    }

    private Object createRetryRecord(Record<Event> record) throws Exception {
        // Get RetryRecord class
        Class<?> retryRecordClass = Class.forName(
                "org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator$RetryRecord");

        // Get constructor
        Constructor<?> constructor = retryRecordClass.getDeclaredConstructor(
                AbstractBatchJobCreator.class, Record.class);
        constructor.setAccessible(true);

        // Create instance
        return constructor.newInstance(sageMakerBatchJobCreator, record);
    }

    private int getRetryCount(Object retryRecord) {
        try {
            Field retryCountField = retryRecord.getClass().getDeclaredField("retryCount");
            retryCountField.setAccessible(true);
            return (int) retryCountField.get(retryRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get retry count", e);
        }
    }

    private Record<Event> getRecordFromRetryRecord(Object retryRecord) {
        try {
            Field recordField = retryRecord.getClass().getDeclaredField("record");
            recordField.setAccessible(true);
            return (Record<Event>) recordField.get(retryRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get record from RetryRecord", e);
        }
    }
}
