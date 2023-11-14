/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.handler;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.http.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.http.configuration.BearerTokenOptions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BearerTokenAuthHttpSinkHandlerTest {

    private String urlString = "http://localhost:8080";

    private OAuthAccessTokenManager oAuthAccessTokenManager;

    private BearerTokenOptions bearerTokenOptions;
    @BeforeEach
    public void setUp() throws Exception{
        bearerTokenOptions = new BearerTokenOptions();
        oAuthAccessTokenManager = mock(OAuthAccessTokenManager.class);
        when(oAuthAccessTokenManager.getAccessToken(bearerTokenOptions)).thenReturn("access_token_test");
    }

    @Test
    public void authenticateTest() {

        HttpAuthOptions.Builder httpAuthOptionsBuilder = new HttpAuthOptions.Builder();
        httpAuthOptionsBuilder.setUrl(urlString);
        httpAuthOptionsBuilder.setHttpClientBuilder(HttpClients.custom());
        httpAuthOptionsBuilder.setHttpClientConnectionManager(PoolingHttpClientConnectionManagerBuilder.create().build());
        httpAuthOptionsBuilder.setClassicHttpRequestBuilder(ClassicRequestBuilder.post());
        Assertions.assertEquals(urlString, new BearerTokenAuthHttpSinkHandler(bearerTokenOptions, new PoolingHttpClientConnectionManager(),oAuthAccessTokenManager).authenticate(httpAuthOptionsBuilder).getUrl());
    }
}
