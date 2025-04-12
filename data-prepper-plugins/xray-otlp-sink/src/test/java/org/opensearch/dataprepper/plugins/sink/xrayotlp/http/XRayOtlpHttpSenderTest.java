/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class XRayOtlpHttpSenderTest {

    private SigV4Signer mockSigner;
    private SdkHttpClient mockHttpClient;
    private XRayOtlpHttpSender sender;

    @BeforeEach
    void setUp() {
        mockSigner = mock(SigV4Signer.class);
        mockHttpClient = mock(SdkHttpClient.class);
        sender = new XRayOtlpHttpSender(mockSigner, mockHttpClient);
    }

    @Test
    void testSend_successfulRequest_logsInfo() throws Exception {
        byte[] payload = "test-payload".getBytes();

        SdkHttpFullRequest signedRequest = mock(SdkHttpFullRequest.class);
        when(mockSigner.signRequest(payload)).thenReturn(signedRequest);

        HttpExecuteResponse mockResponse = mock(HttpExecuteResponse.class);
        when(mockResponse.httpResponse()).thenReturn(SdkHttpResponse.builder().statusCode(200).build());

        ExecutableHttpRequest executableRequest = mock(ExecutableHttpRequest.class);
        when(executableRequest.call()).thenReturn(mockResponse);
        when(mockHttpClient.prepareRequest(any(HttpExecuteRequest.class))).thenReturn(executableRequest);

        assertDoesNotThrow(() -> sender.send(payload));

        verify(mockSigner).signRequest(payload);
        verify(mockHttpClient).prepareRequest(any(HttpExecuteRequest.class));
    }

    @Test
    void testSend_httpError_logsWarning() throws Exception {
        byte[] payload = "test-payload".getBytes();

        SdkHttpFullRequest signedRequest = mock(SdkHttpFullRequest.class);
        when(mockSigner.signRequest(payload)).thenReturn(signedRequest);

        HttpExecuteResponse mockResponse = mock(HttpExecuteResponse.class);
        when(mockResponse.httpResponse()).thenReturn(SdkHttpResponse.builder().statusCode(500).build());

        ExecutableHttpRequest executableRequest = mock(ExecutableHttpRequest.class);
        when(executableRequest.call()).thenReturn(mockResponse);
        when(mockHttpClient.prepareRequest(any(HttpExecuteRequest.class))).thenReturn(executableRequest);

        sender.send(payload);

        verify(mockSigner).signRequest(payload);
        verify(mockHttpClient).prepareRequest(any(HttpExecuteRequest.class));
    }

    @Test
    void testSend_exceptionDuringSend_logsError() throws Exception {
        byte[] payload = "test-payload".getBytes();

        when(mockSigner.signRequest(payload)).thenThrow(new RuntimeException("signing failed"));

        assertDoesNotThrow(() -> sender.send(payload));
        verify(mockSigner).signRequest(payload);
    }

    @Test
    void testClose_closesHttpClient() throws IOException {
        sender.close();
        verify(mockHttpClient).close();
    }
}
