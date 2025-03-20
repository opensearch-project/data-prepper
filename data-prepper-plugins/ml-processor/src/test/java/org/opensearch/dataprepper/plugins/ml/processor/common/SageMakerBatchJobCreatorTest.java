/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor.common;

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
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml.processor.client.S3ClientFactory;
import org.opensearch.dataprepper.plugins.ml.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml.processor.util.RetryUtil;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.opensearch.dataprepper.plugins.ml.processor.common.AbstractBatchJobCreator.NUMBER_OF_FAILED_BATCH_JOBS_CREATION;
import static org.opensearch.dataprepper.plugins.ml.processor.common.AbstractBatchJobCreator.NUMBER_OF_SUCCESSFUL_BATCH_JOBS_CREATION;

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
    private MockedStatic<S3ClientFactory> mockedFactory;

    private SageMakerBatchJobCreator sageMakerBatchJobCreator;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Counter counter;;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://offlinebatch/sagemaker/output");
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
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

        mockedFactory.when(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier)).thenReturn(s3Client);

        sageMakerBatchJobCreator = spy(new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
    }

    @Test
    void testCreateMLBatchJob_Success() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        mockStatic(RetryUtil.class);
        when(RetryUtil.retryWithBackoff(any())).thenReturn(true);

        sageMakerBatchJobCreator.createMLBatchJob(Arrays.asList(record));

        verify(sageMakerBatchJobCreator, times(1)).incrementSuccessCounter();
        mockedFactory.verify(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier), times(1));
    }

    @Test
    void testCreateMLBatchJob_Failure() {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);

        when(event.getJsonNode()).thenReturn(OBJECT_MAPPER.createObjectNode()
                .put("bucket", "test-bucket")
                .put("key", "input.jsonl"));

        mockStatic(RetryUtil.class);
        when(RetryUtil.retryWithBackoff(any())).thenReturn(false);

        sageMakerBatchJobCreator.createMLBatchJob(Arrays.asList(record));

        verify(sageMakerBatchJobCreator, times(1)).incrementFailureCounter();
        mockedFactory.verify(() -> S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier), times(1));
    }

}
