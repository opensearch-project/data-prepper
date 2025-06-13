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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdatePipelineBaseHandlerTest {
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
    void setUp() throws IOException {
        updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider);
        lenient().when(httpExchange.getResponseBody()).thenReturn(outputStream);
        lenient().when(httpExchange.getResponseHeaders()).thenReturn(headers);
        lenient().doNothing().when(httpExchange).sendResponseHeaders(anyInt(), anyLong());
    }

    @ParameterizedTest
    @ValueSource(strings = {HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE})
    void testUnsupportedMethods(final String method) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(method);

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/test@pipeline",              // Invalid characters
            "/toolongpipelinename12345678901",  // Too long
            "/pipeline.name",                    // Invalid characters
            "/",                                 // Empty name
            ""                                   // Empty path
    })
    void testInvalidPipelineNameInPath(final String invalidPath) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create(invalidPath));

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(httpExchange.getResponseHeaders()).add(eq("Content-Type"), eq("application/json; charset=UTF-8"));
        verify(outputStream).write(any(byte[].class));
        verify(outputStream).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "invalid json",
            "{\"invalid\": \"field\"}",
            "{\"s3paths\": []}",
            "{\"s3paths\": [null]}"
    })
    void testInvalidRequestBody(final String requestBody) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipeline123"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)));
        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).write(any(byte[].class));
        verify(outputStream).close();
    }

    @Test
    void testValidRequestWithoutS3Region() throws IOException {


        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipeline123"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(
                "{\"s3paths\": [\"s3://bucket1/path1\"]}".getBytes(StandardCharsets.UTF_8)));

        UpdatePipelineHandler updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider);

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).write(any(byte[].class));
        verify(outputStream).close();
    }

    @Test
    void testInvalidS3Region() throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipeline123"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(
                "{\"s3paths\": [\"s3://bucket1/path1\"], \"s3region\": \"invalid-region\"}".getBytes(StandardCharsets.UTF_8)));

        String errorMessage = "Failed to parse request body: Invalid region provided in the request body";
        String expectedResponse = String.format("{\"error\": \"%s\"}", errorMessage);
        
        UpdatePipelineHandler updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider);
        updatePipelineHandler.handle(httpExchange);

        verify(outputStream).write(eq(expectedResponse.getBytes(StandardCharsets.UTF_8)));
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), eq((long)expectedResponse.length()));
        verify(httpExchange.getResponseHeaders()).add(eq("Content-Type"), eq("application/json; charset=UTF-8"));
        verify(outputStream).close();
    }


    /*@Test
    void testMultipleValidS3Paths() throws IOException {
        // Setup mock responses for S3 client

        String testRegion = "us-east-1";

        // Setup HTTP exchange
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/pipeline123"));
        String payload = "{\"s3paths\": [\"s3://bucket1/path1\", \"s3://bucket2/path2\"], \"s3region\": \"" + testRegion + "\"}";
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));

        // Execute request
        UpdatePipelineHandler updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider);
        updatePipelineHandler.handle(httpExchange);

        // Verify response
        String expectedResponse = "{\"message\": \"Pipeline configuration updated successfully\", \"pipeline\": \"pipeline123\"}";
        verify(outputStream).write(expectedResponse.getBytes(StandardCharsets.UTF_8));
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), eq(Long.valueOf(expectedResponse.getBytes(StandardCharsets.UTF_8).length)));
        verify(httpExchange.getResponseHeaders()).add(eq("Content-Type"), eq("application/json; charset=UTF-8"));
        verify(outputStream).close();
    }*/
}