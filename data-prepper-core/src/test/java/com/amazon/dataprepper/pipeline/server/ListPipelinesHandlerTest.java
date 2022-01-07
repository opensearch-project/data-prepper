/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.pipeline.Pipeline;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ListPipelinesHandlerTest {

    @Test
    public void testGivenNoPipelinesThenResponseWritten() throws IOException {
        DataPrepper dataPrepper = mock(DataPrepper.class);
        HttpExchange httpExchange = mock(HttpExchange.class);
        Headers headers = mock(Headers.class);
        OutputStream outputStream = mock(OutputStream.class);
        Map<String, Pipeline> transformationPipelines = new HashMap<>();

        when(dataPrepper.getTransformationPipelines())
                .thenReturn(transformationPipelines);

        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);

        ListPipelinesHandler handler = new ListPipelinesHandler(dataPrepper);

        handler.handle(httpExchange);

        verify(headers, times(1))
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream, times(1))
                .write(any(byte[].class));
        verify(outputStream, times(1))
                .close();
    }

    @Test
    public void testGivenPipelinesThenResponseWritten() throws IOException {
        DataPrepper dataPrepper = mock(DataPrepper.class);
        HttpExchange httpExchange = mock(HttpExchange.class);
        Headers headers = mock(Headers.class);
        OutputStream outputStream = mock(OutputStream.class);
        Pipeline pipeline = mock(Pipeline.class);
        Map<String, Pipeline> transformationPipelines = new HashMap<>();
        transformationPipelines.put("Pipeline A", pipeline);
        transformationPipelines.put("Pipeline B", pipeline);
        transformationPipelines.put("Pipeline C", pipeline);

        when(dataPrepper.getTransformationPipelines())
                .thenReturn(transformationPipelines);

        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);

        ListPipelinesHandler handler = new ListPipelinesHandler(dataPrepper);

        handler.handle(httpExchange);

        verify(headers, times(1))
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream, times(1))
                .write(any(byte[].class));
        verify(outputStream, times(1))
                .close();
    }

    @Test
    public void testGivenExceptionThrownThenErrorResponseWrittern() throws IOException {
        HttpExchange httpExchange = mock(HttpExchange.class);
        OutputStream outputStream = mock(OutputStream.class);

        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);

        ListPipelinesHandler handler = new ListPipelinesHandler(null);
        handler.handle(httpExchange);

        verify(httpExchange, times(1))
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(outputStream, times(1))
                .close();
    }
}