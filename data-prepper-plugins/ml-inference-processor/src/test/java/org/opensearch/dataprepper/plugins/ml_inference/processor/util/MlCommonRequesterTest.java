/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MlCommonRequesterTest {
    @Mock
    private HttpClientExecutor mockHttpClientExecutor;

    @Mock
    private MLProcessorConfig mockMLProcessorConfig;

    @Mock
    private AwsCredentialsSupplier mockAwsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions mockAwsAuthenticationOptions;

    @Mock
    private HttpExecuteResponse mockResponse;

    @Mock
    private AwsCredentialsProvider mockAwsCredentialsProvider;

    @Mock
    private AwsCredentials mockAwsCredentials;

    private MlCommonRequester mlCommonRequester;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockMLProcessorConfig.getHostUrl()).thenReturn("http://localhost:9200");
        when(mockMLProcessorConfig.getModelId()).thenReturn("test-model");
        when(mockMLProcessorConfig.getActionType()).thenReturn(ActionType.BATCH_PREDICT);
        when(mockMLProcessorConfig.getAwsAuthenticationOptions()).thenReturn(mockAwsAuthenticationOptions);
        when(mockAwsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(mockAwsCredentialsSupplier.getProvider(any())).thenReturn(mockAwsCredentialsProvider);
        when(mockAwsCredentialsProvider.resolveCredentials()).thenReturn(mockAwsCredentials);

        mlCommonRequester = new MlCommonRequester(Aws4Signer.create(), mockMLProcessorConfig, mockAwsCredentialsSupplier, mockHttpClientExecutor);
    }

    @Test
    void testSendRequestToMLCommons_SuccessfulResponse() throws Exception {
        // Arrange
        when(mockResponse.httpResponse()).thenReturn(mock(SdkHttpResponse.class));
        when(mockResponse.httpResponse().statusCode()).thenReturn(200);
        when(mockHttpClientExecutor.execute(any(HttpExecuteRequest.class))).thenReturn(mockResponse);

        // Act
        mlCommonRequester.sendRequestToMLCommons("{\"data\":\"test\"}");

        // Assert
        ArgumentCaptor<HttpExecuteRequest> captor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        verify(mockHttpClientExecutor).execute(captor.capture());

        HttpExecuteRequest capturedRequest = captor.getValue();

        assertEquals(SdkHttpMethod.POST, capturedRequest.httpRequest().method());
        assertEquals(URI.create("http://localhost:9200/_plugins/_ml/models/test-model/_batch_predict"), capturedRequest.httpRequest().getUri());

        verify(mockResponse, times(2)).httpResponse();
    }

    @Test
    void testSendRequestToMLCommons_FailureResponse() throws Exception {
        // Arrange
        when(mockResponse.httpResponse()).thenReturn(mock(SdkHttpResponse.class));
        when(mockResponse.httpResponse().statusCode()).thenReturn(500);
        when(mockHttpClientExecutor.execute(any(HttpExecuteRequest.class))).thenReturn(mockResponse);

        // Act & Assert
        RuntimeException exception = assertThrows(
            RuntimeException.class, () -> mlCommonRequester.sendRequestToMLCommons("{\"data\":\"test\"}")
        );

        assertTrue(exception.getMessage().contains("Failed to execute HTTP request using the ML Commons model"));
    }

    @Test
    public void testReadStream_successfulResponse() throws Exception {
        // Arrange
        String responseBody = "Test successful response";

        // Create a real SdkHttpResponse
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder()
                .statusCode(200)
                .putHeader("Content-Type", "application/json")
                .build();
        // Create a real AbortableInputStream with response data
        AbortableInputStream inputStream = AbortableInputStream.create(
                new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8))
        );

        // Build a real HttpExecuteResponse
        HttpExecuteResponse httpExecuteResponse = HttpExecuteResponse.builder()
                .response(sdkHttpResponse)
                .responseBody(inputStream)
                .build();

        when(mockHttpClientExecutor.execute(any(HttpExecuteRequest.class))).thenReturn(httpExecuteResponse);

        // Call handleHttpResponse to indirectly test readStream
        mlCommonRequester.sendRequestToMLCommons("{\"data\":\"test\"}");

        // Assert
        ArgumentCaptor<HttpExecuteRequest> captor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        verify(mockHttpClientExecutor).execute(captor.capture());
        HttpExecuteRequest capturedRequest = captor.getValue();
        assertEquals(SdkHttpMethod.POST, capturedRequest.httpRequest().method());
        assertEquals(URI.create("http://localhost:9200/_plugins/_ml/models/test-model/_batch_predict"), capturedRequest.httpRequest().getUri());
    }
}
