/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender.NON_RETRYABLE_STATUS_CODES;

class OtlpHttpSenderTest {

    private static final byte[] PAYLOAD = "test-otlp-payload".getBytes(StandardCharsets.UTF_8);
    private static final String ERROR_BODY = "{\"error\": \"Something went wrong\"}";

    private OtlpSinkConfig mockConfig;
    private SigV4Signer mockSigner;
    private OkHttpClient mockHttpClient;
    private Sleeper mockSleeper;
    private OtlpSinkMetrics mockSinkMetrics;
    private OtlpHttpSender target;

    @BeforeEach
    void setUp() {
        System.setProperty("aws.accessKeyId", "dummy");
        System.setProperty("aws.secretAccessKey", "dummy");

        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(mockConfig.getMaxRetries()).thenReturn(3);

        mockSigner = mock(SigV4Signer.class);
        mockHttpClient = mock(OkHttpClient.class);
        mockSleeper = mock(Sleeper.class);
        mockSinkMetrics = mock(OtlpSinkMetrics.class);

        target = new OtlpHttpSender(mockConfig, mockSinkMetrics, mockSigner, mockHttpClient, mockSleeper);
    }

    @AfterEach
    void cleanUp() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void testSend_successfulResponse() throws IOException {
        SdkHttpFullRequest mockSignedRequest = mock(SdkHttpFullRequest.class);
        when(mockSignedRequest.getUri()).thenReturn(HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(mockSignedRequest.headers()).thenReturn(Map.of("Authorization", Collections.singletonList("signed-header")));

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(mockSignedRequest);

        Call mockCall = mock(Call.class);
        Response mockResponse = new Response.Builder()
                .request(new Request.Builder().url("https://xray.us-west-2.amazonaws.com/v1/traces").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create("", MediaType.get("application/x-protobuf")))
                .build();

        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);

        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_doesNotRetryOnNonRetryable4xxResponses() throws IOException {
        final SdkHttpFullRequest mockSignedRequest = mock(SdkHttpFullRequest.class);
        when(mockSignedRequest.getUri()).thenReturn(HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(mockSignedRequest.headers()).thenReturn(Map.of());
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(mockSignedRequest);

        final okhttp3.Request okHttpRequest = new Request.Builder()
                .url("https://xray.us-west-2.amazonaws.com/v1/traces")
                .build();

        for (int statusCode : NON_RETRYABLE_STATUS_CODES) {
            final String responseBodyText = "Non-retryable error from server";

            final Response mockResponse = new Response.Builder()
                    .request(okHttpRequest)
                    .protocol(Protocol.HTTP_1_1)
                    .code(statusCode)
                    .message("Client Error")
                    .body(ResponseBody.create(responseBodyText, MediaType.get("application/json")))
                    .build();

            final Call mockCall = mock(Call.class);
            when(mockCall.execute()).thenReturn(mockResponse);
            when(mockHttpClient.newCall(any())).thenReturn(mockCall);

            assertDoesNotThrow(() -> target.send(PAYLOAD), "Should not throw on non-retryable status " + statusCode);
            verify(mockHttpClient, times(1)).newCall(any());

            reset(mockHttpClient); // reset between iterations
        }
    }

    @Test
    void testSend_retryOnFailure_thenSuccess() throws IOException {
        SdkHttpFullRequest mockSignedRequest = mock(SdkHttpFullRequest.class);
        when(mockSignedRequest.getUri()).thenReturn(HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(mockSignedRequest.headers()).thenReturn(Map.of());

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(mockSignedRequest);

        Call mockCall1 = mock(Call.class);
        Call mockCall2 = mock(Call.class);

        when(mockHttpClient.newCall(any()))
                .thenReturn(mockCall1)
                .thenReturn(mockCall2);

        when(mockCall1.execute()).thenThrow(new IOException("first attempt failed"));
        Response successResponse = new Response.Builder()
                .request(new Request.Builder().url("https://xray.us-west-2.amazonaws.com/v1/traces").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create("", MediaType.get("application/x-protobuf")))
                .build();
        when(mockCall2.execute()).thenReturn(successResponse);

        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_failsAfterAllRetries() throws IOException {
        SdkHttpFullRequest mockSignedRequest = mock(SdkHttpFullRequest.class);
        when(mockSignedRequest.getUri()).thenReturn(HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(mockSignedRequest.headers()).thenReturn(Map.of());

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(mockSignedRequest);

        Call mockCall = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenThrow(new IOException("always fail"));

        assertThrows(RuntimeException.class, () -> target.send(PAYLOAD));
    }

    @Test
    void testSend_throwsIOException_on500ResponseWithBody() throws IOException {
        // Mock signed request
        SdkHttpFullRequest sdkRequest = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(sdkRequest);

        // Build actual OkHttp request (we need this to inject into the mocked response)
        okhttp3.Request okHttpRequest = new Request.Builder()
                .url(sdkRequest.getUri().toString())
                .build();

        // Mock 500 response with body
        Response mockResponse = new Response.Builder()
                .code(500)
                .message("Internal Server Error")
                .request(okHttpRequest)
                .protocol(Protocol.HTTP_1_1)
                .body(ResponseBody.create(ERROR_BODY, MediaType.get("application/json")))
                .build();

        var call = mock(okhttp3.Call.class);
        when(mockHttpClient.newCall(any(Request.class))).thenReturn(call);
        when(call.execute()).thenReturn(mockResponse);

        // Run test
        assertThrows(RuntimeException.class, () -> target.send(PAYLOAD));
    }

    @Test
    void testInterruptedExceptionDuringRetryThrowsRuntimeException() throws IOException, InterruptedException {
        final SdkHttpFullRequest signedRequest = mock(SdkHttpFullRequest.class);

        when(mockSigner.signRequest(any())).thenReturn(signedRequest);
        when(signedRequest.getUri()).thenReturn(URI.create("https://example.com"));
        when(signedRequest.headers()).thenReturn(Map.of());

        final Call mockCall = mock(Call.class);
        when(mockCall.execute()).thenThrow(new IOException("boom"));
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);

        doThrow(new InterruptedException("interrupted")).when(mockSleeper).sleep(anyInt());

        target = new OtlpHttpSender(mockConfig, mockSinkMetrics, mockSigner, mockHttpClient, mockSleeper);

        final RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                target.send(PAYLOAD)
        );

        assertTrue(thrown.getMessage().contains("Retry interrupted"));
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testSend_partialSuccessResponse_logsWarning() throws IOException {
        final ExportTraceServiceResponse responseProto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(5)
                        .setErrorMessage("Some spans were rejected due to invalid format")
                        .build())
                .build();

        final byte[] responseBytes = responseProto.toByteArray();

        final SdkHttpFullRequest sdkRequest = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(sdkRequest);

        final okhttp3.Request okHttpRequest = new Request.Builder()
                .url(sdkRequest.getUri().toString())
                .build();

        final Response mockResponse = new Response.Builder()
                .request(okHttpRequest)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(responseBytes, MediaType.get("application/x-protobuf")))
                .build();

        final Call mockCall = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);

        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_partialSuccessEmpty_logsInfo() throws IOException {
        final ExportTraceServiceResponse responseProto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(0)
                        .setErrorMessage("")  // Empty = no warning
                        .build())
                .build();

        final byte[] responseBytes = responseProto.toByteArray();

        final SdkHttpFullRequest sdkRequest = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(sdkRequest);

        final okhttp3.Request okHttpRequest = new Request.Builder()
                .url(sdkRequest.getUri().toString())
                .build();

        final Response mockResponse = new Response.Builder()
                .request(okHttpRequest)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(responseBytes, MediaType.get("application/x-protobuf")))
                .build();

        final Call mockCall = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);

        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_successResponseWithNonEmptyBodyNoPartialSuccess_logsInfo() throws IOException {
        // Manually add an unknown field to make the serialized body non-empty
        final UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder()
                .addField(123, Field.newBuilder().addVarint(42).build()) // dummy varint
                .build();

        final ExportTraceServiceResponse responseProto = ExportTraceServiceResponse.newBuilder()
                .mergeUnknownFields(unknownFields)
                .build();

        final byte[] responseBytes = responseProto.toByteArray();
        assertTrue(responseBytes.length > 0); // sanity check

        final SdkHttpFullRequest sdkRequest = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(sdkRequest);

        final okhttp3.Request okHttpRequest = new Request.Builder()
                .url(sdkRequest.getUri().toString())
                .build();

        final Response mockResponse = new Response.Builder()
                .request(okHttpRequest)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(responseBytes, MediaType.get("application/x-protobuf")))
                .build();

        final Call mockCall = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);

        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_invalidProtoResponse_logsError() throws IOException {
        // Build a response with invalid OTLP proto data (random bytes)
        byte[] invalidProtoBytes = "this-is-not-valid-proto".getBytes(StandardCharsets.UTF_8);

        // Mock the signed request
        final SdkHttpFullRequest sdkRequest = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();

        when(mockSigner.signRequest(PAYLOAD)).thenReturn(sdkRequest);

        // Build OkHttp request to satisfy response builder
        final okhttp3.Request okHttpRequest = new Request.Builder()
                .url(sdkRequest.getUri().toString())
                .build();

        // Create mock response with invalid proto payload
        final Response mockResponse = new Response.Builder()
                .request(okHttpRequest)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(invalidProtoBytes, MediaType.get("application/x-protobuf")))
                .build();

        final Call mockCall = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(mockCall);
        when(mockCall.execute()).thenReturn(mockResponse);

        // Should not throw, just logs error
        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testDefaultConstructorInitializesDefaults() {
        target = new OtlpHttpSender(mockConfig, mockSinkMetrics);

        // Reflection to assert internal fields (not great, but useful for unit validation)
        assertNotNull(getPrivateField(target, "signer"));
        assertNotNull(getPrivateField(target, "httpClient"));
        assertNotNull(getPrivateField(target, "sleeper"));
    }

    private Object getPrivateField(Object instance, String fieldName) {
        try {
            var field = instance.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(instance);
        } catch (Exception e) {
            fail("Could not access field: " + fieldName);
            return null;
        }
    }

}
