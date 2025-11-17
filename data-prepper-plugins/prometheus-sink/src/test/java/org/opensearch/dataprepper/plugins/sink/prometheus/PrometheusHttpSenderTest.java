/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus;

import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import static com.linecorp.armeria.common.MediaTypeNames.X_PROTOBUF;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.sink.SinkMetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;

public class PrometheusHttpSenderTest {
    private static final int TEST_CONNECTION_TIMEOUT_MILLIS = 60_000;
    private static final int TEST_IDLE_TIMEOUT_MILLIS = 60_000;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private SinkMetrics sinkMetrics;
    @Mock
    private PrometheusSinkConfiguration sinkConfig;
    @Mock
    private WebClient webClient;

    @Mock
    private HttpStatus httpStatus;
    @Mock
    private HttpData httpData;
    @Mock
    private HttpResponse httpResponse;
    @Mock
    private AggregatedHttpResponse aggregatedHttpResponse;
    @Mock
    private CompletableFuture<AggregatedHttpResponse> completableFuture;

    private PrometheusHttpSender prometheusHttpSender;

    @BeforeEach
    void setUp() {
        httpStatus = mock(HttpStatus.class);
        httpData = mock(HttpData.class);
        aggregatedHttpResponse = mock(AggregatedHttpResponse.class);
        httpResponse = mock(HttpResponse.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        sinkMetrics = mock(SinkMetrics.class);
        webClient = mock(WebClient.class);
        sinkConfig = mock(PrometheusSinkConfiguration.class);
        when(sinkConfig.getUrl()).thenReturn("https://testurl.test");
        doNothing().when(sinkMetrics).recordRequestSize(any(Integer.class));
        when(sinkConfig.getEncoding()).thenReturn(CompressionOption.SNAPPY);
        when(sinkConfig.getConnectionTimeout()).thenReturn(Duration.ofMillis(TEST_CONNECTION_TIMEOUT_MILLIS));
        when(sinkConfig.getIdleTimeout()).thenReturn(Duration.ofMillis(TEST_IDLE_TIMEOUT_MILLIS));
        when(sinkConfig.getContentType()).thenReturn(X_PROTOBUF);
        when(sinkConfig.getRemoteWriteVersion()).thenReturn("0.1.0");
        completableFuture = CompletableFuture.supplyAsync(() -> {
            return aggregatedHttpResponse;
        });
        when(httpResponse.aggregate()).thenReturn(completableFuture);
        when(webClient.execute(any(HttpRequest.class))).thenReturn(httpResponse);
    }

    private PrometheusHttpSender createObjectUnderTest() {
        return new PrometheusHttpSender(awsCredentialsSupplier, webClient, sinkConfig, sinkMetrics);
    }

    @Test
    public void testHttpSenderWithoutSignerWithFailureReturnCode() {
        final byte[] bytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(httpStatus.code()).thenReturn(400);
        when(httpData.array()).thenReturn(bytes);
        when(aggregatedHttpResponse.status()).thenReturn(httpStatus);
        when(aggregatedHttpResponse.content()).thenReturn(httpData);
        prometheusHttpSender = createObjectUnderTest();
        final byte[] payloadBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        PrometheusPushResult result = prometheusHttpSender.pushToEndpoint(payloadBytes);
        assertFalse(result.isSuccess());
        assertThat(result.getStatusCode(), equalTo(400));
    }
    
    @Test
    public void testHttpSenderWithoutSignerWithSuccessfulReturnCode() {
        final byte[] bytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        when(httpStatus.code()).thenReturn(200);
        when(httpData.array()).thenReturn(bytes);
        when(aggregatedHttpResponse.status()).thenReturn(httpStatus);
        when(aggregatedHttpResponse.content()).thenReturn(httpData);
        prometheusHttpSender = createObjectUnderTest();
        final byte[] payloadBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        PrometheusPushResult result = prometheusHttpSender.pushToEndpoint(payloadBytes);
        assertTrue(result.isSuccess());
        assertThat(result.getStatusCode(), equalTo(200));
    }
    
    @Test
    public void testHttpSenderWithSigner() {
        when(webClient.execute(any(HttpRequest.class))).thenThrow(new RuntimeException("Exception"));
        prometheusHttpSender = createObjectUnderTest();
        final byte[] payloadBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        PrometheusPushResult result = prometheusHttpSender.pushToEndpoint(payloadBytes);
        assertFalse(result.isSuccess());
        assertThat(result.getStatusCode(), equalTo(0));
    }
    
}
