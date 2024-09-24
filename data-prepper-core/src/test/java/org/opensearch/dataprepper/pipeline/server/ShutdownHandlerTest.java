/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.DataPrepper;
import org.opensearch.dataprepper.DataPrepperShutdownOptions;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
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
        lenient().when(exchange.getRequestHeaders())
                .thenReturn(new Headers());
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

    @Nested
    class WithoutQueryParameters {
        @BeforeEach
        void setUp() {
            when(exchange.getRequestURI()).thenReturn(URI.create("/shutdown"));
        }

        @Test
        public void testWhenShutdownWithPostRequestThenResponseWritten() throws IOException {
            when(exchange.getRequestMethod())
                    .thenReturn(HttpMethod.POST);

            shutdownHandler.handle(exchange);

            ArgumentCaptor<DataPrepperShutdownOptions> shutdownOptionsArgumentCaptor = ArgumentCaptor.forClass(DataPrepperShutdownOptions.class);
            verify(dataPrepper, times(1))
                    .shutdownPipelines(shutdownOptionsArgumentCaptor.capture());
            verify(exchange, times(1))
                    .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq(0L));
            verify(responseBody, times(1))
                    .close();
            verify(dataPrepper, times(1))
                    .shutdownServers();

            DataPrepperShutdownOptions actualShutdownOptions = shutdownOptionsArgumentCaptor.getValue();
            assertThat(actualShutdownOptions.getBufferDrainTimeout(), nullValue());
            assertThat(actualShutdownOptions.getBufferReadTimeout(), nullValue());
        }

        @Test
        public void testHandleException() throws IOException {
            when(exchange.getRequestMethod())
                    .thenReturn(HttpMethod.POST);
            doThrow(RuntimeException.class).when(dataPrepper).shutdownPipelines(any(DataPrepperShutdownOptions.class));

            shutdownHandler.handle(exchange);

            verify(exchange, times(1))
                    .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
            verify(responseBody, times(1))
                    .close();
        }
    }

    @Nested
    class WithoutShutdownQueryParameters {
        @BeforeEach
        void setUp() {
            when(exchange.getRequestURI()).thenReturn(URI.create("/shutdown?bufferReadTimeout=1500ms&bufferDrainTimeout=20s"));
        }

        @Test
        public void testWhenShutdownWithPostRequestThenResponseWritten() throws IOException {
            when(exchange.getRequestMethod())
                    .thenReturn(HttpMethod.POST);

            shutdownHandler.handle(exchange);

            final ArgumentCaptor<DataPrepperShutdownOptions> shutdownOptionsArgumentCaptor = ArgumentCaptor.forClass(DataPrepperShutdownOptions.class);
            verify(dataPrepper, times(1))
                    .shutdownPipelines(shutdownOptionsArgumentCaptor.capture());
            verify(exchange, times(1))
                    .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq(0L));
            verify(responseBody, times(1))
                    .close();
            verify(dataPrepper, times(1))
                    .shutdownServers();

            final DataPrepperShutdownOptions actualShutdownOptions = shutdownOptionsArgumentCaptor.getValue();
            assertThat(actualShutdownOptions.getBufferDrainTimeout(), equalTo(Duration.ofSeconds(20)));
            assertThat(actualShutdownOptions.getBufferReadTimeout(), equalTo(Duration.ofMillis(1500)));
        }

        @Test
        public void testHandleException() throws IOException {
            when(exchange.getRequestMethod())
                    .thenReturn(HttpMethod.POST);
            doThrow(RuntimeException.class).when(dataPrepper).shutdownPipelines(any(DataPrepperShutdownOptions.class));

            shutdownHandler.handle(exchange);

            verify(exchange, times(1))
                    .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
            verify(responseBody, times(1))
                    .close();
        }
    }
}
