/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;

import javax.ws.rs.HttpMethod;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdatePipelineHandlerTest {
    @Mock
    private PipelinesProvider pipelinesProvider;
    @Mock
    private HttpExchange httpExchange;
    @Mock
    private OutputStream outputStream;
    @Mock
    private Headers headers;

    private UpdatePipelineHandler updatePipelineHandler;

    @BeforeEach
    void setUp() {
        updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider);
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE})
    void testUnsupportedMethods(final String method) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(method);
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream).close();
    }

    @Test
    void testInvalidPipelineNameInPath() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/invalid/path"));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testInvalidRequestBody() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipelines/test-pipeline"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream("invalid json".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testMissingS3PathsInRequestBody() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipelines/test-pipeline"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream("{\"invalid\": \"field\"}".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testEmptyS3PathsArrayInRequestBody() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipelines/test-pipeline"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream("{\"s3paths\": []}".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testNullS3PathInArrayInRequestBody() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipelines/test-pipeline"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream("{\"s3paths\": [null]}".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testMultipleValidS3Paths() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipelines/test-pipeline"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(
                "{\"s3paths\": [\"s3://bucket1/path1\", \"s3://bucket2/path2\"]}".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream).close();
    }
}