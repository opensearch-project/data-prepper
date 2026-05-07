/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest;

import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianBearerTokenAuthConfig;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class BearerTokenInterceptor implements ClientHttpRequestInterceptor {

    private final AtlassianBearerTokenAuthConfig authConfig;

    public BearerTokenInterceptor(AtlassianBearerTokenAuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        request.getHeaders().setBearerAuth(authConfig.getBearerToken());
        return execution.execute(request, body);
    }
}
