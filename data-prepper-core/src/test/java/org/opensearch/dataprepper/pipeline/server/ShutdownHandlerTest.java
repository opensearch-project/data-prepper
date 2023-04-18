/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.DataPrepper;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ShutdownHandlerTest {
    @Mock
    private DataPrepper dataPrepper;

    @InjectMocks
    private ShutdownHandler shutdownHandler;

    @Mock
    private HttpExchange exchange;

    @Mock
    private OutputStream responseBody;

    @BeforeEach
    public void beforeEach() {
        when(exchange.getResponseBody())
                .thenReturn(responseBody);
    }

    @Test
    public void testWhenShutdownWithPostRequestThenResponseWritten() throws IOException {
        when(exchange.getRequestMethod())
                .thenReturn(HttpMethod.POST);

        shutdownHandler.handle(exchange);

        verify(dataPrepper, times(1))
                .shutdownPipelines();
        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq(0L));
        verify(responseBody, times(1))
                .close();
        verify(dataPrepper, times(1))
                .shutdownServers();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.PATCH, HttpMethod.PUT })
    public void testWhenShutdownWithProhibitedHttpMethodThenErrorResponseWritten(String httpMethod) throws IOException {
        when(exchange.getRequestMethod())
                .thenReturn(httpMethod);

        shutdownHandler.handle(exchange);

        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(responseBody, times(1))
                .close();
    }

    @Test
    public void testHandleException() throws IOException {
        when(exchange.getRequestMethod())
                .thenReturn(HttpMethod.POST);
        doThrow(RuntimeException.class).when(dataPrepper).shutdownPipelines();

        shutdownHandler.handle(exchange);

        verify(exchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(responseBody, times(1))
                .close();
    }
}
