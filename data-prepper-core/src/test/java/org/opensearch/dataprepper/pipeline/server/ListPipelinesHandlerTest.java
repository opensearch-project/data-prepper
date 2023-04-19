/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelinesProvider;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ListPipelinesHandlerTest {
    @Mock
    private PipelinesProvider pipelinesProvider;
    @Mock
    private HttpExchange httpExchange;

    @Mock
    private OutputStream outputStream;

    @BeforeEach
    public void beforeEach() {
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);
    }

    private ListPipelinesHandler createObjectUnderTest() {
        return new ListPipelinesHandler(pipelinesProvider);
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenNoPipelinesThenResponseWritten(String httpMethod) throws IOException {
        final Headers headers = mock(Headers.class);
        final Map<String, Pipeline> transformationPipelines = new HashMap<>();

        when(pipelinesProvider.getTransformationPipelines())
                .thenReturn(transformationPipelines);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        final ListPipelinesHandler handler = createObjectUnderTest();

        handler.handle(httpExchange);

        verify(headers)
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .write(any(byte[].class));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenPipelinesThenResponseWritten(String httpMethod) throws IOException {
        final Headers headers = mock(Headers.class);
        final Pipeline pipeline = mock(Pipeline.class);
        final Map<String, Pipeline> transformationPipelines = new HashMap<>();
        transformationPipelines.put("Pipeline A", pipeline);
        transformationPipelines.put("Pipeline B", pipeline);
        transformationPipelines.put("Pipeline C", pipeline);

        when(pipelinesProvider.getTransformationPipelines())
                .thenReturn(transformationPipelines);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        final ListPipelinesHandler handler = createObjectUnderTest();

        handler.handle(httpExchange);

        verify(headers)
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .write(any(byte[].class));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.PUT })
    public void testGivenProhibitedHttpMethodThenErrorResponseWritten(String httpMethod) throws IOException {
        final ListPipelinesHandler handler = createObjectUnderTest();

        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenExceptionThrownThenErrorResponseWritten(String httpMethod) throws IOException {
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        pipelinesProvider = null;
        final ListPipelinesHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(outputStream)
                .close();
    }
}
