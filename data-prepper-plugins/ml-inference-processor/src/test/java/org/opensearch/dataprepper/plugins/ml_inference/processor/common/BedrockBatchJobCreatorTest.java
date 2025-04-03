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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
import static org.opensearch.dataprepper.plugins.ml_inference.processor.common.AbstractBatchJobCreator.OBJECT_MAPPER;


public class BedrockBatchJobCreatorTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PluginMetrics pluginMetrics;

    private BedrockBatchJobCreator bedrockBatchJobCreator;
    private Counter counter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/output");
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
        bedrockBatchJobCreator = spy(new BedrockBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
    }

    @Test
    void testCreateMLBatchJob_Success() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(true);

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

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(false);

            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            verify(bedrockBatchJobCreator, times(1)).incrementFailureCounter();
            assertTrue(exception.getMessage().contains("Failed to process the following records"));
        }
    }

    @Test
    void testInterruptedExceptionHandling() throws InterruptedException {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedStatic = mockStatic(RetryUtil.class)) {
            mockedStatic.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(false);

            Thread.currentThread().interrupt();
            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                bedrockBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            assertTrue(Thread.interrupted()); // Ensure interrupted flag is reset
            assertTrue(exception.getMessage().contains("Failed to process the following records"));
        }
    }
}
