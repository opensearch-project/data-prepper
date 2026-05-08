/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.RemoteConnectorExecutor;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BedrockConnectorBatchPredictorTest {

    @Mock
    private RemoteConnectorExecutor connectorExecutor;
    @Mock
    private MLProcessorConfig mlProcessorConfig;
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock
    private MLBatchJobCreator jobNameSource;

    private BedrockConnectorBatchPredictor predictor;

    @BeforeEach
    void setUp() {
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationOptions.getJobStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/JobRole");
        when(mlProcessorConfig.getOutputPath()).thenReturn("s3://output-bucket/results");
        when(jobNameSource.generateJobName()).thenReturn("batch-job-test");
        predictor = new BedrockConnectorBatchPredictor(connectorExecutor, mlProcessorConfig, jobNameSource);
    }

    @Test
    void predict_parametersIncludeJobStsRoleArn() {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://bucket/input.jsonl");

            @SuppressWarnings("unchecked")
            final ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
            verify(connectorExecutor).executeAction(eq(ConnectorActionType.BATCH_PREDICT), captor.capture());
            assertThat(captor.getValue().get("roleArn"), is("arn:aws:iam::123456789012:role/JobRole"));
        }
    }

    @Test
    void predict_parametersIncludeRegion() {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://bucket/input.jsonl");

            @SuppressWarnings("unchecked")
            final ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
            verify(connectorExecutor).executeAction(eq(ConnectorActionType.BATCH_PREDICT), captor.capture());
            assertThat(captor.getValue().get("region"), is("us-east-1"));
        }
    }

    @Test
    void predict_parametersIncludeInputAndOutputConfig() {
        try (MockedStatic<RetryUtil> retryUtil = mockStatic(RetryUtil.class)) {
            retryUtil.when(() -> RetryUtil.retryWithBackoffWithResult(any(), any()))
                    .thenAnswer(invocation -> {
                        final Runnable task = invocation.getArgument(0);
                        task.run();
                        return new RetryUtil.RetryResult(true, null, 1);
                    });

            predictor.predict("s3://input-bucket/data.jsonl");

            @SuppressWarnings("unchecked")
            final ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
            verify(connectorExecutor).executeAction(eq(ConnectorActionType.BATCH_PREDICT), captor.capture());
            final Map<String, String> params = captor.getValue();
            assertThat(params.get("inputDataConfig").contains("s3://input-bucket/data.jsonl"), is(true));
            assertThat(params.get("outputDataConfig").contains("s3://output-bucket/results"), is(true));
            assertThat(params.get("jobName"), is("batch-job-test"));
        }
    }
}
