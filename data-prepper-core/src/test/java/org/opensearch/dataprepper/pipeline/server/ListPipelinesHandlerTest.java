/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.DataPrepper;
import org.opensearch.dataprepper.pipeline.Pipeline;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.Test;

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
    private HttpExchange httpExchange;

    @Mock
    private OutputStream outputStream;

    @BeforeEach
    public void beforeEach() {
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);
        when(httpExchange.getRequestMethod())
                .thenReturn("GET");
    }

    @Test
    public void testGivenNoPipelinesThenResponseWritten() throws IOException {
        final DataPrepper dataPrepper = mock(DataPrepper.class);
        final Headers headers = mock(Headers.class);
        final Map<String, Pipeline> transformationPipelines = new HashMap<>();

        when(dataPrepper.getTransformationPipelines())
                .thenReturn(transformationPipelines);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);

        final ListPipelinesHandler handler = new ListPipelinesHandler(dataPrepper);

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

    @Test
    public void testGivenPipelinesThenResponseWritten() throws IOException {
        final DataPrepper dataPrepper = mock(DataPrepper.class);
        final Headers headers = mock(Headers.class);
        final Pipeline pipeline = mock(Pipeline.class);
        final Map<String, Pipeline> transformationPipelines = new HashMap<>();
        transformationPipelines.put("Pipeline A", pipeline);
        transformationPipelines.put("Pipeline B", pipeline);
        transformationPipelines.put("Pipeline C", pipeline);

        when(dataPrepper.getTransformationPipelines())
                .thenReturn(transformationPipelines);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);

        final ListPipelinesHandler handler = new ListPipelinesHandler(dataPrepper);

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

    @Test
    public void testGivenProhibitedHttpMethodThenErrorResponseWritten() throws IOException {
        final DataPrepper dataPrepper = mock(DataPrepper.class);

        when(httpExchange.getRequestMethod())
                .thenReturn("DELETE");

        final ListPipelinesHandler handler = new ListPipelinesHandler(dataPrepper);

        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream)
                .close();
    }

    @Test
    public void testGivenExceptionThrownThenErrorResponseWritten() throws IOException {
        final ListPipelinesHandler handler = new ListPipelinesHandler(null);
        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(outputStream)
                .close();
    }
}
