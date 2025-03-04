/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultEncryptionHttpHandlerTest {
    @Mock
    private HttpExchange httpExchange;
    @Mock
    private OutputStream outputStream;
    @Mock
    private EncryptionRotationHandler encryptionRotationHandler;

    @BeforeEach
    void setUp() {
        when(httpExchange.getResponseBody()).thenReturn(outputStream);
    }

    @Test
    void testInternalServerError_when_handle_rotation_throws() throws IOException {
        final String encryptionId = UUID.randomUUID().toString();
        when(encryptionRotationHandler.getEncryptionId()).thenReturn(encryptionId);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("http://localhost:4900/" + encryptionId));
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.POST);
        doThrow(RuntimeException.class).when(encryptionRotationHandler).handleRotation();
        final DefaultEncryptionHttpHandler handler = new DefaultEncryptionHttpHandler(
                Set.of(encryptionRotationHandler));

        handler.handle(httpExchange);

        verify(encryptionRotationHandler).handleRotation();
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), anyLong());
        verify(outputStream)
                .close();
    }

    @Test
    void testHandleRotation_when_uri_path_ends_with_encryption_id() throws IOException {
        final String encryptionId = UUID.randomUUID().toString();
        when(encryptionRotationHandler.getEncryptionId()).thenReturn(encryptionId);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("http://localhost:4900/" + encryptionId));
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.POST);
        final DefaultEncryptionHttpHandler handler = new DefaultEncryptionHttpHandler(
                Set.of(encryptionRotationHandler));

        handler.handle(httpExchange);

        verify(encryptionRotationHandler).handleRotation();
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .close();
    }

    @Test
    void testSkipHandleRotation_when_uri_path_does_not_end_with_encryption_id() throws IOException {
        final String encryptionId = UUID.randomUUID().toString();
        when(encryptionRotationHandler.getEncryptionId()).thenReturn(encryptionId);
        when(httpExchange.getRequestURI()).thenReturn(URI.create("http://localhost:4900/" + encryptionId + "diff"));
        when(httpExchange.getRequestMethod()).thenReturn(HttpMethod.POST);
        final DefaultEncryptionHttpHandler handler = new DefaultEncryptionHttpHandler(
                Set.of(encryptionRotationHandler));

        handler.handle(httpExchange);

        verify(encryptionRotationHandler, times(0)).handleRotation();
        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.PUT, HttpMethod.GET})
    void testGivenProhibitedHttpMethodThenErrorResponseWritten(String httpMethod) throws IOException {
        final DefaultEncryptionHttpHandler handler = new DefaultEncryptionHttpHandler(Collections.emptySet());

        when(httpExchange.getRequestMethod()).thenReturn(httpMethod);

        handler.handle(httpExchange);

        verify(httpExchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream).close();
    }
}