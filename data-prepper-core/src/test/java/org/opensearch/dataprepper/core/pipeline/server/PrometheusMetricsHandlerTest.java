/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrometheusMetricsHandlerTest {

    @Mock
    PrometheusMeterRegistry meterRegistry;

    @InjectMocks
    PrometheusMetricsHandler metricsHandler;

    @Mock
    HttpExchange exchange;

    @Mock
    OutputStream responseBody;

    @BeforeEach
    public void beforeEach() {
        when(exchange.getResponseBody())
                .thenReturn(responseBody);
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testWhenUseValidHttpMethodThenResponseWritten(String httpMethod) throws IOException {
        final Headers headers = mock(Headers.class);
        when(exchange.getResponseHeaders())
                .thenReturn(headers);

        final String testString = "I am a string used in a test";
        when(meterRegistry.scrape())
                .thenReturn(testString);

        when(exchange.getRequestMethod())
                .thenReturn(httpMethod);

        metricsHandler.handle(exchange);


        verify(headers, times(1))
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));

        final byte[] response = testString.getBytes(StandardCharsets.UTF_8);
        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq((long) response.length));
        verify(responseBody, times(1))
                .write(response);
        verify(responseBody, times(1))
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.PUT })
    public void testWhenUseProhibitedHttpMethodThenErrorResponseWritten(String httpMethod) throws IOException {
        when(exchange.getRequestMethod())
                .thenReturn(httpMethod);
        metricsHandler.handle(exchange);

        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(responseBody, times(1))
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testHandleException(String httpMethod) throws IOException {
        when(exchange.getRequestMethod())
                .thenReturn(httpMethod);
        metricsHandler.handle(exchange);

        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(responseBody, times(1))
                .close();
    }

}
