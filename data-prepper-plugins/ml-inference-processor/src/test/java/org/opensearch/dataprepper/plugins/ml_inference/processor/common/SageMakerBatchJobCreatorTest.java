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
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

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

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/sagemaker/output");
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        final EventKey sourceKey = eventKeyFactory.createEventKey("key");
        when(mlProcessorConfig.getInputKey()).thenReturn(sourceKey);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
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

        // sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
    }

    @Test
    void testCreateMLBatchJob_Success() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class);
             MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(true);
            mockedS3ClientFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier)).thenReturn(s3Client);

            sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(null);

            sageMakerBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());

            verify(sageMakerBatchJobCreator, times(1)).incrementSuccessCounter();
            mockedS3ClientFactory.verify(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier), times(1));
        }
    }

    @Test
    void testCreateMLBatchJob_Failure() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        try (MockedStatic<RetryUtil> mockedRetryUtil = mockStatic(RetryUtil.class);
             MockedStatic<S3ClientFactory> mockedS3ClientFactory = mockStatic(S3ClientFactory.class)) {
            sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
            mockedRetryUtil.when(() -> RetryUtil.retryWithBackoff(any(), any())).thenReturn(false);

            MLBatchJobException exception = assertThrows(MLBatchJobException.class, () -> {
                sageMakerBatchJobCreator.createMLBatchJob(Arrays.asList(record), new ArrayList<>());
            });

            verify(sageMakerBatchJobCreator, times(1)).incrementFailureCounter();
            mockedS3ClientFactory.verify(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier), times(1));
            assertTrue(exception.getMessage().contains("Failed to create SageMaker batch job"));
        }
    }

}
