/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.client.S3ClientFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.NUMBER_OF_FAILED_BATCH_JOBS_CREATION;
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

    private SageMakerBatchJobCreator sageMakerBatchJobCreator;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private Counter counter;
    private ConcurrentLinkedQueue<Record<Event>> batch_records;
    private ConcurrentLinkedQueue<Record<Event>> processedBatchRecords;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/sagemaker/output");
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        final EventKey sourceKey = eventKeyFactory.createEventKey("key");
        when(mlProcessorConfig.getInputKey()).thenReturn(sourceKey);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        counter = mock(Counter.class);
        when(pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION)).thenReturn(counter);

        try (MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {
            mockedS3ClientFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier)).thenReturn(s3Client);

            // Create a spy of the real object
            sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));

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
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(true);

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
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(true);
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
    void testProcessRemainingBatch() throws Exception {
        // Arrange
        Event event = createMockEvent("test-bucket", "input.jsonl");
        batch_records.add(new Record<>(event));

        // Mock RetryUtil to return success
        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class)) {
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(true);
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
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(false);

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
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(false);
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            // Act - trigger batch processing with a batch that will fail
            sageMakerBatchJobCreator.checkAndProcessBatch();

            // Assert
            verify(counter, times(1)).increment(); // Failure counter
            assertEquals(100, processedBatchRecords.size()); // Record should still be added but with failure tag
            assertTrue(batch_records.isEmpty());
        }
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
}
