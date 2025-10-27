/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.OBJECT_MAPPER;


public class BedrockBatchJobCreatorTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DlqPushHandler dlqPushHandler;

    @Mock
    private PluginSetting pluginSetting;

    private BedrockBatchJobCreator bedrockBatchJobCreator;
    private Counter counter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/output");
        when(mlProcessorConfig.getRetryTimeWindow()).thenReturn(DEFAULT_RETRY_WINDOW);
        counter = new Counter() {
            @Override
            public void increment(double v) {}

            @Override
            public double count() {
                return 0;
            }

            @Override
            public Id getId() {
                return null;
            }
        };
        when(pluginMetrics.counter(NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_FAILED_BATCH_JOBS_CREATION)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_FAILED_IN_BATCH_JOB)).thenReturn(counter);
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_SUCCEEDED_IN_BATCH_JOB)).thenReturn(counter);
        when(pluginSetting.getPipelineName()).thenReturn("pipeline_bedrock");
        when(pluginSetting.getName()).thenReturn("pipeline_bedrock");
        bedrockBatchJobCreator = spy(new BedrockBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler));
    }

    @Test
    void testCreateMLBatchJob_Success() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(true, null, 1));

            bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            verify(bedrockBatchJobCreator, times(1)).incrementSuccessCounter();
        }
    }

    @Test
    void testCreateMLBatchJob_Failure() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));
        when(event.toJsonString()).thenReturn("event");

        when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(500, "errorMessage") , 1));

            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            verify(bedrockBatchJobCreator, times(1)).incrementFailureCounter();
            assertTrue(exception.getMessage().contains("Failed to process 1 records out of 1 total records"));
        }
    }

    @Test
    void testInterruptedExceptionHandling() throws InterruptedException {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        when(event.toJsonString()).thenReturn("event");
        when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(500, "errorMessage") , 1));

            Thread.currentThread().interrupt();
            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            assertTrue(Thread.interrupted()); // Ensure interrupted flag is reset
            assertTrue(exception.getMessage().contains("Failed to process 1 records out of 1 total records"));
        }
    }

    @Test
    void testCreateMLBatchJob_IllegalArgumentException() throws Exception {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", ""));

        when(event.toJsonString()).thenReturn("event");
        when(dlqPushHandler.getDlqPluginSetting()).thenReturn(pluginSetting);

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any())).thenReturn(new RetryUtil.RetryResult(true, null, 1));

            Thread.currentThread().interrupt();
            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            assertTrue(Thread.interrupted()); // Ensure interrupted flag is reset
            assertTrue(exception.getMessage().contains("Failed to process 1 records out of 1 total records"));
        }
    }

    @Test
    void testCreateMLBatchJob_Throttled() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            // First attempt - gets throttled
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "throttled"), 1));

            List<Record<Event>> resultRecords = new ArrayList<>();
            bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), resultRecords);

            // Verify record was added to throttled queue and not to result records
            assertTrue(resultRecords.isEmpty());
            assertEquals(1, bedrockBatchJobCreator.getThrottledRecords().size());

            // Process throttled records
            bedrockBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);

            // Verify throttled record was processed
            assertEquals(1, bedrockBatchJobCreator.getThrottledRecords().size());
            BedrockBatchJobCreator.RetryRecord throttledRecord = bedrockBatchJobCreator.getThrottledRecords().peek();
            assertNotNull(throttledRecord);
            assertEquals(1, throttledRecord.getRetryCount());
        }
    }

    @Test
    void testCreateMLBatchJob_ThrottledMultipleTimes() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            // Configure to return throttled response multiple times
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "throttled"), 1));

            List<Record<Event>> resultRecords = new ArrayList<>();

            // First attempt
            bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), resultRecords);
            assertEquals(1, bedrockBatchJobCreator.getThrottledRecords().size());

            // Process throttled records - should get throttled again
            bedrockBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);

            // Verify retry count increased
            BedrockBatchJobCreator.RetryRecord throttledRecord = bedrockBatchJobCreator.getThrottledRecords().peek();
            assertNotNull(throttledRecord);
            assertEquals(1, throttledRecord.getRetryCount());

            // Process again
            bedrockBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);
            throttledRecord = bedrockBatchJobCreator.getThrottledRecords().peek();
            assertNotNull(throttledRecord);
            assertEquals(2, throttledRecord.getRetryCount());
        }
    }

    @Test
    void testCreateMLBatchJob_ThrottledThenSuccess() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            // First return throttled, then success
            mockedStatic.when(() -> RetryUtil.retryWithBackoffWithResult(any(Runnable.class), any()))
                    .thenReturn(new RetryUtil.RetryResult(false, new MLBatchJobException(429, "throttled"), 1))
                    .thenReturn(new RetryUtil.RetryResult(true, null, 1));

            List<Record<Event>> resultRecords = new ArrayList<>();

            // First attempt - gets throttled
            bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), resultRecords);
            assertTrue(resultRecords.isEmpty());
            assertEquals(1, bedrockBatchJobCreator.getThrottledRecords().size());

            // Process throttled records - should succeed
            bedrockBatchJobCreator.addProcessedBatchRecordsToResults(resultRecords);

            // Verify record was processed successfully
            assertTrue(bedrockBatchJobCreator.getThrottledRecords().isEmpty());
            assertEquals(1, resultRecords.size());
            verify(bedrockBatchJobCreator, times(1)).incrementSuccessCounter();
        }
    }
}
