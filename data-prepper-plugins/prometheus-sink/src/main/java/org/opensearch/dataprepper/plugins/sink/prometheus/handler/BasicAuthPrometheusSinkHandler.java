/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.handler;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.prometheus.util.PrometheusSinkUtil;

/**
 * * This class handles Basic Authentication
 */
public class BasicAuthPrometheusSinkHandler implements MultiAuthPrometheusSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final String username;

    private final String password;

    public BasicAuthPrometheusSinkHandler(final String username,
                                    final String password,
                                    final HttpClientConnectionManager httpClientConnectionManager){
        this.httpClientConnectionManager = httpClientConnectionManager;
        this.username = username;
        this.password = password;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder  httpAuthOptionsBuilder) {
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(PrometheusSinkUtil.getHttpHostByURL(PrometheusSinkUtil.getURLByUrlString(httpAuthOptionsBuilder.getUrl())));
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(httpAuthOptionsBuilder.getUrl()))
                .setDefaultCredentialsProvider(provider));
        return httpAuthOptionsBuilder.build();
    }
}
