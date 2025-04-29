/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private Consumer<Integer> mockSleeper;
    private Function<byte[], Optional<byte[]>> mockGzipCompressor;
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
        mockSleeper = mock(ThreadSleeper.class);
        mockSinkMetrics = mock(OtlpSinkMetrics.class);

        mockGzipCompressor = mock(GzipCompressor.class);
        when(mockGzipCompressor.apply(any()))
                .thenAnswer(invocation -> Optional.of((byte[]) invocation.getArgument(0)));

        target = new OtlpHttpSender(
                mockConfig, mockSinkMetrics,
                mockGzipCompressor, mockSigner,
                mockHttpClient, mockSleeper
        );
    }

    @AfterEach
    void cleanUp() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void testSend_successfulResponse() throws IOException {
        // Arrange
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(signed.getUri()).thenReturn(
                HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(signed.headers()).thenReturn(
                Map.of("Authorization", Collections.singletonList("signed-header")));
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Call call = mock(Call.class);
        final Response resp = new Response.Builder()
                .request(new Request.Builder().url(signed.getUri().toString()).build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(
                        new byte[0],
                        MediaType.get("application/x-protobuf")))
                .build();

        when(mockHttpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(resp);

        // Act & Assert
        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_doesNotRetryOnNonRetryable4xxResponses() throws IOException {
        // Arrange
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(signed.getUri()).thenReturn(
                HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(signed.headers()).thenReturn(Map.of());
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Request okReq = new Request.Builder()
                .url(signed.getUri().toString())
                .build();

        for (final int status : NON_RETRYABLE_STATUS_CODES) {
            final Response resp = new Response.Builder()
                    .request(okReq)
                    .protocol(Protocol.HTTP_1_1)
                    .code(status)
                    .message("Client Error")
                    .body(ResponseBody.create(
                            ERROR_BODY.getBytes(StandardCharsets.UTF_8),
                            MediaType.get("application/json")))
                    .build();
            final Call call = mock(Call.class);
            when(mockHttpClient.newCall(any())).thenReturn(call);
            when(call.execute()).thenReturn(resp);

            assertDoesNotThrow(() -> target.send(PAYLOAD));
            verify(mockHttpClient, times(1)).newCall(any());
            reset(mockHttpClient);
        }
    }

    @Test
    void testSend_retryOnFailure_thenSuccess() throws IOException {
        // Arrange
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(signed.getUri()).thenReturn(
                HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(signed.headers()).thenReturn(Map.of());
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Call first = mock(Call.class);
        final Call second = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(first, second);
        when(first.execute()).thenThrow(new IOException("first attempt failed"));
        final Response success = new Response.Builder()
                .request(new Request.Builder().url(signed.getUri().toString()).build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(
                        new byte[0],
                        MediaType.get("application/x-protobuf")))
                .build();
        when(second.execute()).thenReturn(success);

        // Act & Assert
        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_throwsIOException_whenFailsAfterAllRetries() throws IOException {
        // Arrange
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(signed.getUri()).thenReturn(
                HttpUrl.get("https://xray.us-west-2.amazonaws.com/v1/traces").uri());
        when(signed.headers()).thenReturn(Map.of());
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Call alwaysFail = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(alwaysFail);
        when(alwaysFail.execute()).thenThrow(new IOException("always fail"));

        // Act & Assert
        final IOException ex = assertThrows(IOException.class, () -> target.send(PAYLOAD));
        assertEquals("always fail", ex.getMessage());
    }

    @Test
    void testSend_throwsIOException_on500ResponseWithBody() throws IOException {
        final SdkHttpFullRequest signed = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://example.com"))
                .putHeader("Content-Type", "application/json")
                .build();
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Request okReq = new Request.Builder()
                .url(signed.getUri().toString()).build();
        final Call call = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(call);

        final Response resp500 = new Response.Builder()
                .request(okReq)
                .protocol(Protocol.HTTP_1_1)
                .code(500)
                .message("Internal Server Error")
                .body(ResponseBody.create(
                        ERROR_BODY.getBytes(StandardCharsets.UTF_8),
                        MediaType.get("application/json")))
                .build();
        when(call.execute()).thenReturn(resp500);

        assertThrows(IOException.class, () -> target.send(PAYLOAD));
    }

    @Test
    void testSend_wrapsInterruptedExceptionDuringRetryAsIOException() throws IOException {
        // Arrange: first attempt throws IOException, sleeper throws at retry
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(mockSigner.signRequest(any())).thenReturn(signed);
        when(signed.getUri()).thenReturn(URI.create("https://example.com"));
        when(signed.headers()).thenReturn(Map.of());

        final Call call = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenThrow(new IOException("boom"));

        doThrow(new RuntimeException("sleep failed"))
                .when(mockSleeper).accept(anyInt());

        target = new OtlpHttpSender(
                mockConfig, mockSinkMetrics,
                mockGzipCompressor, mockSigner,
                mockHttpClient, mockSleeper
        );

        // Act & Assert
        final IOException ex = assertThrows(IOException.class, () -> target.send(PAYLOAD));
        assertEquals("Sender failed to sleep before retrying.", ex.getMessage());
        assertEquals("sleep failed", ex.getCause().getMessage());
    }

    @Test
    void testSend_partialSuccessResponse_logsWarning() throws IOException {
        // Arrange
        final ExportTraceServiceResponse proto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(5)
                        .setErrorMessage("Some spans were rejected due to invalid format")
                        .build())
                .build();
        final byte[] bytes = proto.toByteArray();

        final SdkHttpFullRequest signed = SdkHttpFullRequest.builder()
                .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                .uri(URI.create("https://xray.us-west-2.amazonaws.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        final Request okReq = new Request.Builder()
                .url(signed.getUri().toString())
                .build();
        final Call call = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(call);
        when(call.execute()).thenReturn(new Response.Builder()
                .request(okReq)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(bytes, MediaType.get("application/x-protobuf")))
                .build());

        // Act & Assert
        assertDoesNotThrow(() -> target.send(PAYLOAD));
    }

    @Test
    void testSend_skipsSend_whenGzipCompressionFails() {
        // Arrange: compressor returns empty → send() should return silently
        final Function<byte[], Optional<byte[]>> skipCompressor = p -> Optional.empty();
        target = new OtlpHttpSender(
                mockConfig, mockSinkMetrics,
                skipCompressor, mockSigner,
                mockHttpClient, mockSleeper
        );

        // Act & Assert: no exception, nothing signed or sent
        assertDoesNotThrow(() -> target.send(PAYLOAD));
        verify(mockSigner, never()).signRequest(any());
        verify(mockHttpClient, never()).newCall(any());
    }

    @Test
    void testHandleSuccessfulResponseParseErrorIncrementsError() throws IOException {
        // arrange: build a sender with a pass-through compressor lambda
        target = new OtlpHttpSender(
                mockConfig,
                mockSinkMetrics,
                payload -> Optional.of(payload),
                mockSigner,
                mockHttpClient,
                mockSleeper
        );

        // stub: signer
        final SdkHttpFullRequest signed = mock(SdkHttpFullRequest.class);
        when(signed.getUri()).thenReturn(URI.create("https://example.com"));
        when(signed.headers()).thenReturn(Map.of());
        when(mockSigner.signRequest(PAYLOAD)).thenReturn(signed);

        // stub: http client returns 200 OK but with invalid protobuf bytes
        final Call call = mock(Call.class);
        when(mockHttpClient.newCall(any())).thenReturn(call);
        final byte[] bad = "not-a-proto".getBytes(StandardCharsets.UTF_8);
        final Response resp = new Response.Builder()
                .request(new Request.Builder().url(signed.getUri().toString()).build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create(bad, MediaType.get("application/x-protobuf")))
                .build();
        when(call.execute()).thenReturn(resp);

        // act
        assertDoesNotThrow(() -> target.send(PAYLOAD));

        // assert: parse-failure should have incremented errors
        verify(mockSinkMetrics).incrementErrorsCount();
    }

    @Test
    void testCloseEvictsAndShutdownsOkHttpResources() {
        // arrange: stub out connectionPool and dispatcher on our mockHttpClient
        final ConnectionPool pool = mock(ConnectionPool.class);
        final Dispatcher dispatcher = mock(Dispatcher.class);
        final ExecutorService exec = mock(ExecutorService.class);
        when(mockHttpClient.connectionPool()).thenReturn(pool);
        when(mockHttpClient.dispatcher()).thenReturn(dispatcher);
        when(dispatcher.executorService()).thenReturn(exec);

        // act
        target.close();

        // assert
        verify(pool).evictAll();
        verify(exec).shutdown();
    }

    @Test
    void testDefaultConstructorInitializesDefaults() {
        target = new OtlpHttpSender(mockConfig, mockSinkMetrics);
        assertNotNull(getField(target, "signer"));
        assertNotNull(getField(target, "httpClient"));
        assertNotNull(getField(target, "sleeper"));
    }

    private Object getField(final Object obj, final String name) {
        try {
            final var f = obj.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(obj);
        } catch (Exception e) {
            fail("Could not access " + name);
            return null;
        }
    }
}
