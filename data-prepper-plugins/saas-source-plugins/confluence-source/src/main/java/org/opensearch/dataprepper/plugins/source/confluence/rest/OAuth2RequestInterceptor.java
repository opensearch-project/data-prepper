/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.rest;

import org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceOauthConfig;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class OAuth2RequestInterceptor implements ClientHttpRequestInterceptor {

    private final ConfluenceAuthConfig config;

    public OAuth2RequestInterceptor(ConfluenceAuthConfig config) {
        this.config = config;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        request.getHeaders().setBearerAuth(((ConfluenceOauthConfig) config).getAccessToken());
        return execution.execute(request, body);
    }

}
