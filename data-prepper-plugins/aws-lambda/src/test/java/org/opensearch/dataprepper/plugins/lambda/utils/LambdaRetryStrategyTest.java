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
        assertTrue(LambdaRetryStrategy.isRetryable(429));
        assertTrue(LambdaRetryStrategy.isRetryable(500));
        assertFalse(LambdaRetryStrategy.isRetryable(200));
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

}
