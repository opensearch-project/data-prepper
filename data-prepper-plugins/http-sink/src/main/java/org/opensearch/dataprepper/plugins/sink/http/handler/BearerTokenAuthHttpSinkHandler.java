/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.handler;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.http.FailedHttpResponseInterceptor;

/**
 * * This class handles Bearer Token Authentication
 */
public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    public static final String AUTHORIZATION = "Authorization";

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final String bearerTokenString;

    public BearerTokenAuthHttpSinkHandler(final String bearerTokenString,
                                          final HttpClientConnectionManager httpClientConnectionManager){
        this.bearerTokenString = bearerTokenString;
        this.httpClientConnectionManager = httpClientConnectionManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder httpAuthOptionsBuilder) {
        httpAuthOptionsBuilder.getClassicHttpRequestBuilder().addHeader(AUTHORIZATION,bearerTokenString);
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(httpAuthOptionsBuilder.getUrl())));
        return httpAuthOptionsBuilder.build();
    }
}
