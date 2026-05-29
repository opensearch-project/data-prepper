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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianBearerTokenAuthConfig;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BearerTokenInterceptorTest {

    @Mock
    private AtlassianBearerTokenAuthConfig authConfig;

    @Mock
    private HttpRequest request;

    @Mock
    private ClientHttpRequestExecution execution;

    @Mock
    private ClientHttpResponse response;

    @Test
    void testInterceptSetsBearerAuthHeader() throws IOException {
        final String token = UUID.randomUUID().toString();
        when(authConfig.getBearerToken()).thenReturn(token);
        HttpHeaders headers = new HttpHeaders();
        when(request.getHeaders()).thenReturn(headers);
        when(execution.execute(request, new byte[0])).thenReturn(response);

        BearerTokenInterceptor interceptor = new BearerTokenInterceptor(authConfig);
        interceptor.intercept(request, new byte[0], execution);

        verify(execution).execute(request, new byte[0]);
        assertThat(headers.getFirst(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + token));
    }
}
