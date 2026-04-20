/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.connector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.HttpClientExecutor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AwsConnectorExecutorTest {

    @Mock
    private MLProcessorConfig config;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private HttpClientExecutor httpClientExecutor;
    @Mock
    private AwsAuthenticationOptions awsAuthOptions;
    @Mock
    private HttpExecuteResponse httpResponse;
    @Mock
    private SdkHttpResponse sdkHttpResponse;

    private AwsConnector connector;
    private AwsConnectorExecutor executor;

    @BeforeEach
    void setUp() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_EMBED_V2_MODEL_ID).orElseThrow();
        connector = (AwsConnector) AbstractConnector.fromJson(json);

        when(config.getAwsAuthenticationOptions()).thenReturn(awsAuthOptions);
        when(awsAuthOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthOptions.getAwsStsRoleArn()).thenReturn(null);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        when(awsCredentialsProvider.resolveCredentials()).thenReturn(AwsBasicCredentials.create("accessKey", "secretKey"));

        executor = new AwsConnectorExecutor(connector, config, awsCredentialsSupplier, httpClientExecutor);
    }

    @Test
    void executeAction_batchPredict_sendsRequestToBedrockBatchEndpoint() throws Exception {
        stubHttpResponse(200, "{}");

        executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters());

        final ArgumentCaptor<HttpExecuteRequest> captor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        verify(httpClientExecutor).execute(captor.capture());
        final String url = captor.getValue().httpRequest().getUri().toString();
        assertThat(url, containsString("bedrock"));
        assertThat(url, containsString("us-east-1"));
        assertThat(url, containsString("model-invocation-job"));
    }

    @Test
    void executeAction_predict_sendsRequestToBedrockRuntimeEndpoint() throws Exception {
        stubHttpResponse(200, "{}");

        final Map<String, String> params = new HashMap<>();
        params.put("region", "us-east-1");
        params.put("inputText", "hello world");
        executor.executeAction(ConnectorActionType.PREDICT, params);

        final ArgumentCaptor<HttpExecuteRequest> captor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        verify(httpClientExecutor).execute(captor.capture());
        final String url = captor.getValue().httpRequest().getUri().toString();
        assertThat(url, containsString("bedrock-runtime"));
        assertThat(url, containsString("us-east-1"));
        assertThat(url, containsString("invoke"));
    }

    @Test
    void executeAction_batchPredictStatus_sendsGetRequestWithJobArn() throws Exception {
        stubHttpResponse(200, "{}");

        final Map<String, String> params = new HashMap<>();
        params.put("region", "us-east-1");
        params.put("jobArn", "arn:aws:bedrock:us-east-1:123456789012:model-invocation-job/test-job");
        executor.executeAction(ConnectorActionType.BATCH_PREDICT_STATUS, params);

        final ArgumentCaptor<HttpExecuteRequest> captor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        verify(httpClientExecutor).execute(captor.capture());
        final String url = captor.getValue().httpRequest().getUri().toString();
        assertThat(url, containsString("model-invocation-job"));
    }

    @Test
    void executeAction_whenResponseIs429_throwsMLBatchJobExceptionWithThrottleStatus() throws Exception {
        stubHttpResponse(429, "Too Many Requests");

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getStatusCode(), is(429));
        assertThat(ex.getMessage(), containsString("throttled"));
    }

    @Test
    void executeAction_whenResponseIs400_throwsMLBatchJobExceptionWithClientError() throws Exception {
        stubHttpResponse(400, "Bad Request");

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getStatusCode(), is(400));
        assertThat(ex.getMessage(), containsString("Client error"));
    }

    @Test
    void executeAction_whenResponseIs500_throwsMLBatchJobExceptionWithServerError() throws Exception {
        stubHttpResponse(500, "Internal Server Error");

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getStatusCode(), is(500));
        assertThat(ex.getMessage(), containsString("Server error"));
    }

    @Test
    void executeAction_whenResponseIsUnexpectedNon200_throwsMLBatchJobException() throws Exception {
        stubHttpResponse(204, "No Content");

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getStatusCode(), is(204));
        assertThat(ex.getMessage(), containsString("Unexpected status code"));
    }

    @Test
    void executeAction_whenIOExceptionThrown_throwsMLBatchJobExceptionWithIOMessage() throws Exception {
        when(httpClientExecutor.execute(any())).thenThrow(new IOException("connection refused"));

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getMessage(), containsString("IO issue"));
    }

    @Test
    void executeAction_whenUnexpectedExceptionThrown_throwsMLBatchJobException() throws Exception {
        when(httpClientExecutor.execute(any())).thenThrow(new RuntimeException("unexpected"));

        final MLBatchJobException ex = assertThrows(MLBatchJobException.class,
                () -> executor.executeAction(ConnectorActionType.BATCH_PREDICT, batchPredictParameters()));

        assertThat(ex.getMessage(), containsString("Unexpected error"));
    }

    @Test
    void executeAction_whenActionNotPresentInConnector_throwsIllegalArgumentException() throws Exception {
        final String emptyActionsJson = "{\"name\":\"test\",\"protocol\":\"aws_sigv4\","
                + "\"parameters\":{\"service_name\":\"bedrock\"},\"actions\":[]}";
        final AwsConnector emptyConnector = (AwsConnector) AbstractConnector.fromJson(emptyActionsJson);
        final AwsConnectorExecutor emptyExecutor =
                new AwsConnectorExecutor(emptyConnector, config, awsCredentialsSupplier, httpClientExecutor);

        assertThrows(IllegalArgumentException.class,
                () -> emptyExecutor.executeAction(ConnectorActionType.PREDICT, new HashMap<>()));
    }

    @Test
    void getConnector_returnsAwsConnectorInstance() {
        assertThat(executor.getConnector(), instanceOf(AwsConnector.class));
    }

    // --- helpers ---

    private Map<String, String> batchPredictParameters() {
        final Map<String, String> params = new HashMap<>();
        params.put("region", "us-east-1");
        params.put("jobName", "test-job");
        params.put("roleArn", "arn:aws:iam::123456789012:role/TestRole");
        params.put("inputDataConfig", "{\"s3InputDataConfig\":{\"s3Uri\":\"s3://bucket/input\"}}");
        params.put("outputDataConfig", "{\"s3OutputDataConfig\":{\"s3Uri\":\"s3://bucket/output\"}}");
        return params;
    }

    private void stubHttpResponse(final int statusCode, final String body) throws Exception {
        final AbortableInputStream bodyStream = AbortableInputStream.create(
                new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        when(httpClientExecutor.execute(any())).thenReturn(httpResponse);
        when(httpResponse.httpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.statusCode()).thenReturn(statusCode);
        when(httpResponse.responseBody()).thenReturn(Optional.of(bodyStream));
    }
}
