package org.opensearch.dataprepper.plugins.lambda.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.util.LambdaRetryStrategy;
import org.slf4j.Logger;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.ServiceException;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaRetryStrategyTest {

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private Buffer buffer;

    @Mock
    private LambdaCommonConfig config;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
//        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));
        when(config.getClientOptions()).thenReturn(mock(ClientOptions.class));
        when(config.getClientOptions().getMaxConnectionRetries()).thenReturn(3);
        when(config.getClientOptions().getBaseDelay()).thenReturn(Duration.ofMillis(100));
        when(config.getFunctionName()).thenReturn("testFunction");
        when(config.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
    }

    @Test
    void testIsRetryable() {
        assertTrue(LambdaRetryStrategy.isRetryable(InvokeResponse.builder().statusCode(429).build()));
        assertTrue(LambdaRetryStrategy.isRetryable(InvokeResponse.builder().statusCode(500).build()));
        assertFalse(LambdaRetryStrategy.isRetryable(InvokeResponse.builder().statusCode(200).build()));
        assertFalse(LambdaRetryStrategy.isRetryable(null));
    }

    @Test
    void testIsNonRetryable() {
        assertTrue(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(400).build()));
        assertTrue(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(403).build()));
        assertFalse(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(500).build()));
        assertFalse(LambdaRetryStrategy.isNonRetryable(null));
    }

    @Test
    void testIsTimeoutError() {
        assertTrue(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(408).build()));
        assertTrue(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(429).build()));
        assertFalse(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(200).build()));
    }

    @Test
    void testRetryOrFail_SuccessAfterRetry() throws Exception {
        when(config.getClientOptions().getMaxConnectionRetries()).thenReturn(3);
        when(config.getClientOptions().getBaseDelay()).thenReturn(Duration.ofMillis(100));
        when(config.getFunctionName()).thenReturn("testFunction");

        InvokeRequest mockRequest = mock(InvokeRequest.class);
        when(buffer.getRequestPayload(anyString(), anyString())).thenReturn(mockRequest);

        InvokeResponse failedResponse = InvokeResponse.builder().statusCode(500).build();
        InvokeResponse successResponse = InvokeResponse.builder().statusCode(200).build();

        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(failedResponse))
                .thenReturn(CompletableFuture.completedFuture(successResponse));

        InvokeResponse result = LambdaRetryStrategy.retryOrFail(lambdaAsyncClient, buffer, config, failedResponse, logger);

        assertEquals(200, result.statusCode());
        verify(lambdaAsyncClient, times(2)).invoke(any(InvokeRequest.class));
    }

    @Test
    void testRetryOrFailExhaustedRetries() throws Exception {
        when(config.getClientOptions().getMaxConnectionRetries()).thenReturn(3);
        when(config.getClientOptions().getBaseDelay()).thenReturn(Duration.ofMillis(100));
        when(config.getFunctionName()).thenReturn("testFunction");

        InvokeRequest mockRequest = mock(InvokeRequest.class);
        when(buffer.getRequestPayload(anyString(), anyString())).thenReturn(mockRequest);

        InvokeResponse failedResponse = InvokeResponse.builder().statusCode(500).build();

        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(failedResponse));

        InvokeResponse result = LambdaRetryStrategy.retryOrFail(lambdaAsyncClient, buffer, config, failedResponse, logger);

        assertEquals(500, result.statusCode());
        verify(lambdaAsyncClient, times(3)).invoke(any(InvokeRequest.class));
    }

    @Test
    void testRetryOrFail_NonRetryableResponse() {
        InvokeResponse nonRetryableResponse = InvokeResponse.builder().statusCode(400).build();
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(nonRetryableResponse));
        when(buffer.getRequestPayload(anyString(), anyString())).thenReturn(mock(InvokeRequest.class));

        assertThrows(RuntimeException.class, ()->
                LambdaRetryStrategy.retryOrFail(lambdaAsyncClient, buffer, config, nonRetryableResponse, logger));

        verify(lambdaAsyncClient, times(1)).invoke(any(InvokeRequest.class));
    }

}
