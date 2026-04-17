/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.google.protobuf.MessageLite;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.ResponseHeaders;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.utils.Pair;

import java.net.URI;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OtlpHttpSenderTest {

    private static final byte[] PAYLOAD = "test-otlp".getBytes(StandardCharsets.UTF_8);
    private static final int BATCH_COUNT = 3;

    private OtlpSinkMetrics metrics;
    private SigV4Signer signer;
    private WebClient webClient;
    private Function<byte[], byte[]> gzipCompressor;
    private OtlpHttpSender sender;
    private AwsCredentialsSupplier mockAwsCredSupplier;
    private List<Pair<MessageLite, EventHandle>> traceBatch;
    private List<Pair<MessageLite, EventHandle>> logBatch;
    private EventHandle mockEventHandle1;
    private EventHandle mockEventHandle2;
    private EventHandle mockEventHandle3;

    @BeforeEach
    void setup() {
        metrics = mock(OtlpSinkMetrics.class);
        signer = mock(SigV4Signer.class);
        webClient = mock(WebClient.class);
        gzipCompressor = mock(Function.class);
        mockAwsCredSupplier = mock(AwsCredentialsSupplier.class);

        mockEventHandle1 = mock(EventHandle.class);
        mockEventHandle2 = mock(EventHandle.class);
        mockEventHandle3 = mock(EventHandle.class);

        traceBatch = Arrays.asList(
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle1),
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle2),
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle3)
        );

        logBatch = Arrays.asList(
                Pair.of(ResourceLogs.newBuilder().build(), mockEventHandle1),
                Pair.of(ResourceLogs.newBuilder().build(), mockEventHandle2),
                Pair.of(ResourceLogs.newBuilder().build(), mockEventHandle3)
        );

        when(gzipCompressor.apply(any())).thenReturn(PAYLOAD);
        when(signer.signRequest(any())).thenReturn(
                SdkHttpFullRequest.builder()
                        .method(SdkHttpMethod.POST)
                        .uri(URI.create("https://localhost/v1/traces"))
                        .putHeader("Authorization", "sig")
                        .build()
        );

        sender = new OtlpHttpSender(metrics, gzipCompressor, signer, webClient);
    }

    // --- Trace signal tests ---

    @Test
    void testSend_emptyBatch_returnsEarly() {
        final List<Pair<MessageLite, EventHandle>> emptyBatch = Collections.emptyList();

        when(webClient.execute(any(HttpRequest.class))).thenThrow(new AssertionError("Should not call execute"));

        sender.send(emptyBatch, false);

        verifyNoInteractions(metrics);
    }

    @Test
    void testSend_traces_successfulResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(metrics).incrementPayloadSize(anyLong());
            verify(metrics).incrementPayloadGzipSize(PAYLOAD.length);
            verify(metrics).recordHttpLatency(anyLong());
            verify(metrics).recordResponseCode(200);
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_traces_partialSuccessResponse() {
        final ExportTraceServiceResponse proto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(2)
                        .setErrorMessage("invalid span")
                        .build())
                .build();

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.wrap(proto.toByteArray()))
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(2);
            verify(metrics).incrementRecordsOut(BATCH_COUNT - 2);
            verify(metrics).recordResponseCode(200);
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_traces_partialSuccessWithZeroRejected() {
        final ExportTraceServiceResponse proto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(0)
                        .setErrorMessage("")
                        .build())
                .build();

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.wrap(proto.toByteArray()))
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics, never()).incrementRejectedSpansCount(anyLong());
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(metrics).recordResponseCode(200);
        });
    }

    @Test
    void testSend_parseErrorOnSuccessResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.ofUtf8("not-protobuf"))
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).incrementErrorsCount();
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(metrics).recordResponseCode(200);
            verify(mockEventHandle1).release(true);
        });
    }

    @Test
    void testSend_nonSuccessStatus() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(400), HttpData.ofUtf8("{\"error\":\"bad request\"}"))
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(400);
            verify(metrics).incrementRejectedSpansCount(BATCH_COUNT);
            verify(mockEventHandle1).release(false);
            verify(mockEventHandle2).release(false);
            verify(mockEventHandle3).release(false);
        });
    }

    @Test
    void testSend_nonSuccessStatusWithNullResponseBody() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(500), HttpData.empty())
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(500);
            verify(metrics).incrementRejectedSpansCount(BATCH_COUNT);
            verify(mockEventHandle1).release(false);
        });
    }

    @Test
    void testSend_skipsSendIfGzipFails() {
        sender = new OtlpHttpSender(metrics, ignored -> new byte[0], signer, webClient);

        sender.send(traceBatch, false);

        verify(metrics).incrementFailedSpansCount(BATCH_COUNT);
        verifyNoInteractions(webClient);
        verify(mockEventHandle1).release(false);
    }

    @Test
    void testSend_exceptionDuringSendIncrementsRejected() {
        final HttpResponse failingResponse = HttpResponse.ofFailure(new RuntimeException("send failed"));
        when(webClient.execute(any(HttpRequest.class))).thenReturn(failingResponse);

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(BATCH_COUNT);
            verify(mockEventHandle1).release(false);
        });
    }

    // --- Log signal tests ---

    @Test
    void testSend_logs_successfulResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        sender.send(logBatch, true);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(metrics).recordResponseCode(200);
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_logs_successfulResponseWithNullBody() {
        // Simulate a response where content().array() returns null-like empty
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200))
        );

        sender.send(logBatch, true);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(mockEventHandle1).release(true);
        });
    }

    @Test
    void testSend_logs_partialSuccessResponse() {
        final ExportLogsServiceResponse proto = ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(1)
                        .setErrorMessage("invalid log")
                        .build())
                .build();

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.wrap(proto.toByteArray()))
        );

        sender.send(logBatch, true);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(1);
            verify(metrics).incrementRecordsOut(BATCH_COUNT - 1);
            verify(metrics).recordResponseCode(200);
        });
    }

    @Test
    void testSend_logs_nonSuccessStatus() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(403), HttpData.ofUtf8("forbidden"))
        );

        sender.send(logBatch, true);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(403);
            verify(metrics).incrementRejectedSpansCount(BATCH_COUNT);
            verify(mockEventHandle1).release(false);
        });
    }

    // --- Additional headers test ---

    @Test
    void testSend_withAdditionalHeaders() {
        final Map<String, String> headers = Map.of("x-custom-header", "custom-value");
        sender = new OtlpHttpSender(metrics, gzipCompressor, signer, webClient, headers);

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        sender.send(traceBatch, false);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(BATCH_COUNT);
            verify(signer).signRequest(any());
        });
    }

    // --- Coverage: private method paths ---

    @Test
    void testHandleSuccessfulResponse_nullBytes_logSignal() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod(
                "handleSuccessfulResponse", byte[].class, List.class, boolean.class);
        method.setAccessible(true);

        method.invoke(sender, null, logBatch, true);

        verify(metrics).incrementRecordsOut(BATCH_COUNT);
        verify(mockEventHandle1).release(true);
    }

    @Test
    void testHandleSuccessfulResponse_nullBytes_traceSignal() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod(
                "handleSuccessfulResponse", byte[].class, List.class, boolean.class);
        method.setAccessible(true);

        method.invoke(sender, null, traceBatch, false);

        verify(metrics).incrementRecordsOut(BATCH_COUNT);
        verify(mockEventHandle1).release(true);
    }

    // --- Constructor tests ---

    @Test
    void testConstructor_withNullAdditionalHeaders() {
        final OtlpHttpSender nullHeadersSender = new OtlpHttpSender(metrics, gzipCompressor, signer, webClient, null);
        assertNotNull(nullHeadersSender);

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        nullHeadersSender.send(traceBatch, false);

        await().untilAsserted(() -> verify(metrics).incrementRecordsOut(BATCH_COUNT));
    }

    @Test
    void testConstructor_withDefaultConfig() {
        final OtlpSinkConfig config = mock(OtlpSinkConfig.class);
        when(config.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getFlushTimeoutMillis()).thenReturn(5000L);
        when(config.getAwsRegion()).thenReturn(software.amazon.awssdk.regions.Region.US_WEST_2);
        when(config.getAdditionalHeaders()).thenReturn(Map.of());

        final OtlpHttpSender defaultSender = new OtlpHttpSender(mockAwsCredSupplier, config, metrics);
        assertNotNull(defaultSender);
    }

    @Test
    void testConstructor_withMinimumThresholdConfig() {
        final OtlpSinkConfig config = mock(OtlpSinkConfig.class);
        when(config.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(config.getMaxBatchSize()).thenReturn(0L);
        when(config.getMaxRetries()).thenReturn(0);
        when(config.getFlushTimeoutMillis()).thenReturn(1L);
        when(config.getAwsRegion()).thenReturn(software.amazon.awssdk.regions.Region.US_WEST_2);
        when(config.getAdditionalHeaders()).thenReturn(Map.of());

        final OtlpHttpSender minimalSender = new OtlpHttpSender(mockAwsCredSupplier, config, metrics);
        assertNotNull(minimalSender);
    }
}
