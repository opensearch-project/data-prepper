/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.util.HttpSinkUtil;

import java.io.IOException;
import java.net.URL;

public class WebhookService {
    private final HttpClientBuilder httpClientBuilder;
    private URL url;

    public WebhookService(final String url,
                          final HttpClientBuilder httpClientBuilder){
        this.httpClientBuilder = httpClientBuilder;
        this.url = HttpSinkUtil.getURLByUrlString(url);
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
                    .addResponseInterceptorLast(new FailedHttpResponseInterceptor(url.toString()))
                    .build()
                    .execute(targetHost, classicHttpRequestBuilder.build(), HttpClientContext.create());
        } catch (IOException e) {
            // call backoff incase any exception
        }

    }


}
