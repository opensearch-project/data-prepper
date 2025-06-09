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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.ws.rs.HttpMethod;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
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
    @Mock
    private S3Client s3Client;

    private UpdatePipelineHandler updatePipelineHandler;

    @BeforeEach
    void setUp() {
        updatePipelineHandler = new UpdatePipelineHandler(pipelinesProvider, s3Client);
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

    @ParameterizedTest
    @ValueSource(strings = {
            "/invalid/path/without-extension",
            "/invalid.txt",
            "/path/to/file.yaml",
            "/test.json.bak",
            "/",
            ""
    })
    void testInvalidPipelineNameInPath(final String invalidPath) throws IOException {
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create(invalidPath));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
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
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/test-pipeline.json"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);
        doNothing().when(httpExchange).sendResponseHeaders(any(Integer.class), anyLong());

        updatePipelineHandler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        verify(outputStream).close();
    }

    @Test
    void testMultipleValidS3Paths() throws IOException {
        // Setup mock responses for S3 client
        String content1 = "content1";
        String content2 = "content2";

        when(s3Client.getObject(any(GetObjectRequest.class))).thenAnswer(inv -> {
            GetObjectRequest request = inv.getArgument(0);
            InputStream contentStream;

            if (request.bucket().equals("bucket1") && request.key().equals("path1")) {
                contentStream = new ByteArrayInputStream(content1.getBytes(StandardCharsets.UTF_8));
            } else if (request.bucket().equals("bucket2") && request.key().equals("path2")) {
                contentStream = new ByteArrayInputStream(content2.getBytes(StandardCharsets.UTF_8));
            } else {
                throw new RuntimeException("Invalid bucket/key combination");
            }

            return new ResponseInputStream<>(GetObjectResponse.builder().build(), contentStream);
        });

        // Setup HTTP exchange
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.PUT);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("/test-pipeline.json"));
        when(httpExchange.getRequestBody()).thenReturn(new ByteArrayInputStream(
                "{\"s3paths\": [\"s3://bucket1/path1\", \"s3://bucket2/path2\"]}".getBytes(StandardCharsets.UTF_8)));
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
        when(httpExchange.getResponseHeaders()).thenReturn(headers);

        // Execute request
        updatePipelineHandler.handle(httpExchange);

        // Verify response
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream).write(any(byte[].class));
        verify(outputStream).close();
    }
}