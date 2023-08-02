/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.http.util.HttpSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class WebhookService {

    private static final Logger LOG = LoggerFactory.getLogger(WebhookService.class);

    public static final String HTTP_SINK_SUCCESS_WEBHOOKS = "httpSinkSuccessWebhooks";

    public static final String HTTP_SINK_FAILED_WEBHOOKS = "httpSinkFailedWebhooks";

    private final HttpClientBuilder httpClientBuilder;

    private final Counter httpSinkWebhookSuccessCounter;

    private final Counter httpSinkWebhookFailedCounter;

    private final DefaultHttpRequestRetryStrategy httpRequestRetryStrategy;

    private URL url;

    public WebhookService(final String url,
                          final HttpClientBuilder httpClientBuilder,
                          final PluginMetrics pluginMetrics,
                          final HttpSinkConfiguration httpSinkConfiguration){
        this.httpClientBuilder = httpClientBuilder;
        this.url = HttpSinkUtil.getURLByUrlString(url);
        this.httpSinkWebhookSuccessCounter = pluginMetrics.counter(HTTP_SINK_SUCCESS_WEBHOOKS);
        this.httpSinkWebhookFailedCounter = pluginMetrics.counter(HTTP_SINK_FAILED_WEBHOOKS);
        this.httpRequestRetryStrategy = new DefaultHttpRequestRetryStrategy(httpSinkConfiguration.getMaxUploadRetries(),
                TimeValue.of(httpSinkConfiguration.getHttpRetryInterval()));
    }

    /**
     * * It sends failed dlq data to configured webhook url
     *  @param failedDlqData Failed Dlq data.
     */
    public void pushWebhook(final FailedDlqData failedDlqData) {
        final HttpHost targetHost;
        final CloseableHttpResponse webhookResp;
        targetHost = HttpSinkUtil.getHttpHostByURL(url);
        final ClassicRequestBuilder classicHttpRequestBuilder =
                ClassicRequestBuilder.post().setEntity(failedDlqData.toString()).setUri(url.toString());
        try {
            webhookResp = httpClientBuilder
                    .setRetryStrategy(new DefaultHttpRequestRetryStrategy())
                    .addResponseInterceptorLast(new FailedHttpResponseInterceptor(url.toString()))
                    .build()
                    .execute(targetHost, classicHttpRequestBuilder.build(), HttpClientContext.create());
            httpSinkWebhookSuccessCounter.increment();
        } catch (IOException e) {
            httpSinkWebhookFailedCounter.increment();
            LOG.error("Exception while pushing webhook: ",e);
        }
    }
}
