/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReadinessHandlerTest {

    @Mock
    private PipelinesProvider pipelinesProvider;

    @Mock
    private HttpExchange httpExchange;

    @Mock
    private OutputStream outputStream;

    @Mock
    private Headers headers;

    @BeforeEach
    public void beforeEach() {
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);
    }

    private ReadinessHandler createObjectUnderTest() {
        return new ReadinessHandler(pipelinesProvider);
    }

    @Test
    public void testReturns200WhenAllPipelinesReady() throws IOException {
        final Pipeline pipeline1 = mock(Pipeline.class);
        final Pipeline pipeline2 = mock(Pipeline.class);
        when(pipeline1.isSourceStarted()).thenReturn(true);
        when(pipeline2.isSourceStarted()).thenReturn(true);

        final Map<String, Pipeline> pipelines = new HashMap<>();
        pipelines.put("pipeline-1", pipeline1);
        pipelines.put("pipeline-2", pipeline2);

        when(pipelinesProvider.getTransformationPipelines()).thenReturn(pipelines);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(headers).add(eq("Content-Type"), eq("application/json"));
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq((long) "{\"ready\":true}".getBytes().length));

        final ArgumentCaptor<byte[]> responseCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(outputStream).write(responseCaptor.capture());
        final String responseBody = new String(responseCaptor.getValue());
        assertThat(responseBody, containsString("\"ready\":true"));
        verify(outputStream).close();
    }

    @Test
    public void testReturns503WhenSomePipelinesNotReady() throws IOException {
        final Pipeline readyPipeline = mock(Pipeline.class);
        final Pipeline notReadyPipeline = mock(Pipeline.class);
        when(readyPipeline.isSourceStarted()).thenReturn(true);
        when(notReadyPipeline.isSourceStarted()).thenReturn(false);

        final Map<String, Pipeline> pipelines = new HashMap<>();
        pipelines.put("ready-pipeline", readyPipeline);
        pipelines.put("not-ready-pipeline", notReadyPipeline);

        when(pipelinesProvider.getTransformationPipelines()).thenReturn(pipelines);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(headers).add(eq("Content-Type"), eq("application/json"));

        final ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        final ArgumentCaptor<Long> lengthCaptor = ArgumentCaptor.forClass(Long.class);
        verify(httpExchange).sendResponseHeaders(statusCaptor.capture(), lengthCaptor.capture());
        assertThat(statusCaptor.getValue(), org.hamcrest.Matchers.equalTo(HttpURLConnection.HTTP_UNAVAILABLE));

        final ArgumentCaptor<byte[]> responseCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(outputStream).write(responseCaptor.capture());
        final String responseBody = new String(responseCaptor.getValue());
        assertThat(responseBody, containsString("\"ready\":false"));
        assertThat(responseBody, containsString("not-ready-pipeline"));
        verify(outputStream).close();
    }

    @Test
    public void testReturns503WhenNoPipelinesNotReady() throws IOException {
        final Pipeline notReadyPipeline1 = mock(Pipeline.class);
        final Pipeline notReadyPipeline2 = mock(Pipeline.class);
        when(notReadyPipeline1.isSourceStarted()).thenReturn(false);
        when(notReadyPipeline2.isSourceStarted()).thenReturn(false);

        final Map<String, Pipeline> pipelines = new HashMap<>();
        pipelines.put("pipeline-a", notReadyPipeline1);
        pipelines.put("pipeline-b", notReadyPipeline2);

        when(pipelinesProvider.getTransformationPipelines()).thenReturn(pipelines);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        final ArgumentCaptor<byte[]> responseCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(outputStream).write(responseCaptor.capture());
        final String responseBody = new String(responseCaptor.getValue());
        assertThat(responseBody, containsString("\"ready\":false"));
        assertThat(responseBody, containsString("pipeline-a"));
        assertThat(responseBody, containsString("pipeline-b"));
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_UNAVAILABLE), eq((long) responseCaptor.getValue().length));
        verify(outputStream).close();
    }

    @Test
    public void testReturns503WhenNoPipelinesRegistered() throws IOException {
        final Map<String, Pipeline> pipelines = new HashMap<>();

        when(pipelinesProvider.getTransformationPipelines()).thenReturn(pipelines);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(headers).add(eq("Content-Type"), eq("application/json"));

        final ArgumentCaptor<byte[]> responseCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(outputStream).write(responseCaptor.capture());
        final String responseBody = new String(responseCaptor.getValue());
        assertThat(responseBody, containsString("\"ready\":false"));
        assertThat(responseBody, containsString("no pipelines registered"));
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_UNAVAILABLE), eq((long) responseCaptor.getValue().length));
        verify(outputStream).close();
    }

    @Test
    public void testReturns200WithSinglePipelineReady() throws IOException {
        final Pipeline pipeline = mock(Pipeline.class);
        when(pipeline.isSourceStarted()).thenReturn(true);

        final Map<String, Pipeline> pipelines = new HashMap<>();
        pipelines.put("my-pipeline", pipeline);

        when(pipelinesProvider.getTransformationPipelines()).thenReturn(pipelines);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq((long) "{\"ready\":true}".getBytes().length));
        verify(outputStream).close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.POST, HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.PUT })
    public void testReturns405ForNonGetMethods(String httpMethod) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(httpMethod);

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream).close();
    }

    @Test
    public void testReturns500WhenExceptionThrown() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.GET);
        when(pipelinesProvider.getTransformationPipelines()).thenThrow(new RuntimeException("test exception"));

        final ReadinessHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(outputStream).close();
    }
}
