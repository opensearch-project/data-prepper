/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.handler;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.dataprepper.plugins.sink.http.util.HttpSinkUtil;

import java.net.URL;

import static org.mockito.ArgumentMatchers.any;

public class BasicAuthHttpSinkHandlerTest {

    private MockedStatic<HttpSinkUtil> httpSinkUtilStatic;

    private String urlString = "http://localhost:8080";
    @BeforeEach
    public void setUp() throws Exception{
        URL url = new URL(urlString);
        httpSinkUtilStatic = Mockito.mockStatic(HttpSinkUtil.class);
        httpSinkUtilStatic.when(() -> HttpSinkUtil.getURLByUrlString(any()))
                .thenReturn(url);
        HttpHost targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        httpSinkUtilStatic.when(() -> HttpSinkUtil.getHttpHostByURL(any(URL.class)))
                .thenReturn(targetHost);
    }

    @AfterEach
    public void tearDown() {
        httpSinkUtilStatic.close();
    }

    @Test
    public void authenticateTest() {
        HttpAuthOptions.Builder httpAuthOptionsBuilder = new HttpAuthOptions.Builder();
        httpAuthOptionsBuilder.setUrl(urlString);
        httpAuthOptionsBuilder.setHttpClientBuilder(HttpClients.custom());
        httpAuthOptionsBuilder.setHttpClientConnectionManager(PoolingHttpClientConnectionManagerBuilder.create().build());
        Assertions.assertEquals(urlString, new BasicAuthHttpSinkHandler("test", "test", new PoolingHttpClientConnectionManager()).authenticate(httpAuthOptionsBuilder).getUrl());
    }
}
