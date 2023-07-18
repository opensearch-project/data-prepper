/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebhookServiceTest {

    private static final String TEST_URL = "http://localhost:8080/"+ UUID.randomUUID();
    private HttpClientBuilder httpClientBuilder;
    private PluginMetrics pluginMetrics;
    private HttpSinkConfiguration httpSinkConfiguration;

    private Counter httpSinkWebhookSuccessCounter;

    private Counter httpSinkWebhookFailedCounter;

    private CloseableHttpClient closeableHttpClient;

    private CloseableHttpResponse closeableHttpResponse;

    @BeforeEach
    public void setup() throws IOException {
        this.httpClientBuilder = mock(HttpClientBuilder.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.httpSinkConfiguration = mock(HttpSinkConfiguration.class);
        this.httpSinkWebhookSuccessCounter = mock(Counter.class);
        this.httpSinkWebhookFailedCounter = mock(Counter.class);
        this.closeableHttpClient = mock(CloseableHttpClient.class);
        this.closeableHttpResponse = mock(CloseableHttpResponse.class);

        lenient().when(httpClientBuilder.build()).thenReturn(closeableHttpClient);
        lenient().when(httpClientBuilder.addResponseInterceptorLast(any(FailedHttpResponseInterceptor.class))).thenReturn(httpClientBuilder);
        lenient().when(httpClientBuilder.setRetryStrategy(any(DefaultHttpRequestRetryStrategy.class))).thenReturn(httpClientBuilder);

        when(pluginMetrics.counter(WebhookService.HTTP_SINK_SUCCESS_WEBHOOKS)).thenReturn(httpSinkWebhookSuccessCounter);
        when(pluginMetrics.counter(WebhookService.HTTP_SINK_FAILED_WEBHOOKS)).thenReturn(httpSinkWebhookFailedCounter);
    }


    private WebhookService createObjectUnderTest(){
        return new WebhookService(TEST_URL,httpClientBuilder,pluginMetrics,httpSinkConfiguration);
    }

    @Test
    public void http_sink_webhook_service_test_with_one_webhook_success_push() throws IOException {
        lenient().when(closeableHttpClient.execute(any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenReturn(closeableHttpResponse);
        HttpEndPointResponse httpEndPointResponse = new HttpEndPointResponse(TEST_URL,200);
        FailedDlqData failedDlqData = FailedDlqData.builder().withBufferData("Test Data").withEndPointResponses(httpEndPointResponse).build();
        WebhookService webhookService = createObjectUnderTest();
        webhookService.pushWebhook(failedDlqData);
        verify(httpSinkWebhookSuccessCounter).increment();
    }

    @Test
    public void http_sink_webhook_service_test_with_one_webhook_failed_to_push() throws IOException {
        when(closeableHttpClient.execute(any(HttpHost.class),any(ClassicHttpRequest.class),any(HttpClientContext.class))).thenThrow(new IOException("Internal Server Error"));
        HttpEndPointResponse httpEndPointResponse = new HttpEndPointResponse(TEST_URL,500);
        FailedDlqData failedDlqData = FailedDlqData.builder().withBufferData("Test Data").withEndPointResponses(httpEndPointResponse).build();
        WebhookService webhookService = createObjectUnderTest();
        webhookService.pushWebhook(failedDlqData);
        verify(httpSinkWebhookFailedCounter).increment();
    }
}
