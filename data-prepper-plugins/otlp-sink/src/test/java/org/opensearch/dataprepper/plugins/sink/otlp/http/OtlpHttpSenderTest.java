/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.ResponseHeaders;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalHandler;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalType;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.utils.Pair;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpHttpSenderTest {

    private static final String TRANSACTION_SEARCH_DISABLED_MESSAGE = "The OTLP API is supported with CloudWatch Logs as a Trace Segment "
            + "Destination. Please enable the CloudWatch Logs destination for your traces using the UpdateTraceSegmentDestination API "
            + "(https://docs.aws.amazon.com/xray/latest/api/API_UpdateTraceSegmentDestination.html)";
    private static final byte[] TRANSACTION_SEARCH_DISABLED_BODY = buildTransactionSearchDisabledBody();

    private OtlpSinkMetrics mockMetrics;
    private Function<byte[], byte[]> mockCompressor;
    private SigV4Signer mockSigner;
    private WebClient mockWebClient;
    private OtlpSignalHandler<ResourceSpans> mockHandler;
    private OtlpHttpSender sender;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockMetrics = mock(OtlpSinkMetrics.class);
        mockCompressor = mock(Function.class);
        mockSigner = mock(SigV4Signer.class);
        mockWebClient = mock(WebClient.class);
        mockHandler = mock(OtlpSignalHandler.class);

        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient);
    }

    private static byte[] buildTransactionSearchDisabledBody() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(0x12);
        outputStream.write(0x8d);
        outputStream.write(0x02);
        outputStream.writeBytes(TRANSACTION_SEARCH_DISABLED_MESSAGE.getBytes(UTF_8));
        return outputStream.toByteArray();
    }

    private SdkHttpFullRequest createSignedRequest() {
        return SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create("https://example.com/v1/traces"))
                .putHeader("Content-Type", "application/x-protobuf")
                .build();
    }

    private void setupCompressorAndSigner(final byte[] payload, final byte[] compressed) {
        when(mockHandler.buildRequestPayload(any())).thenReturn(payload);
        when(mockCompressor.apply(payload)).thenReturn(compressed);
        when(mockSigner.signRequest(compressed)).thenReturn(createSignedRequest());
    }

    @Test
    void testSend_emptyBatch_doesNothing() {
        sender.send(Collections.emptyList(), mockHandler, OtlpSignalType.TRACE);

        verify(mockWebClient, never()).execute(any(HttpRequest.class));
    }

    @Test
    void testSend_compressedPayloadEmpty_incrementsFailedAndReleases() {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        when(mockHandler.buildRequestPayload(any())).thenReturn(new byte[]{1, 2, 3});
        when(mockCompressor.apply(any())).thenReturn(new byte[0]);

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        verify(mockMetrics).incrementFailedRecordsCount(1);
        verify(handle).release(false);
    }

    @Test
    void testSend_successfulResponse_incrementsRecordsOut() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        final byte[] responseBody = ExportTraceServiceResponse.getDefaultInstance().toByteArray();
        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.wrap(responseBody)));

        when(mockHandler.parsePartialSuccess(any())).thenReturn(Pair.of(0L, ""));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRecordsOut(1);
            verify(handle).release(true);
        });
    }

    @Test
    void testSend_nonSuccessfulResponse_incrementsRejected() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRejectedRecordsCount(1);
            verify(handle).release(false);
        });
    }

    @Test
    void testSend_partialSuccess_incrementsRejectedCount() throws Exception {
        final EventHandle handle1 = mock(EventHandle.class);
        final EventHandle handle2 = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(
                Pair.of(ResourceSpans.getDefaultInstance(), handle1),
                Pair.of(ResourceSpans.getDefaultInstance(), handle2));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        final byte[] responseBody = new byte[]{1, 2};
        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.wrap(responseBody)));

        when(mockHandler.parsePartialSuccess(any())).thenReturn(Pair.of(1L, "partial error"));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRejectedRecordsCount(1L);
            verify(mockMetrics).incrementRecordsOut(1L);
            verify(mockMetrics).incrementPartialBatchRejectedSignalCount(OtlpSignalType.TRACE.getMetricsLabel(), 1L);
        });
        verify(mockMetrics, never()).incrementCompleteBatchRejectedSignalCount(anyString(), anyLong());
    }

    @Test
    void testSend_responseParseException_incrementsErrorsAndRecordsOut() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        final byte[] responseBody = new byte[]{1, 2};
        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.wrap(responseBody)));

        when(mockHandler.parsePartialSuccess(any())).thenThrow(new RuntimeException("parse error"));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementErrorsCount();
            verify(mockMetrics).incrementRecordsOut(1);
            verify(handle).release(true);
        });
    }

    @Test
    void testSend_executionException_incrementsRejected() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.ofFailure(new RuntimeException("connection failed")));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRejectedRecordsCount(1);
            verify(mockMetrics).incrementCompleteBatchRejectedSignalCount(OtlpSignalType.TRACE.getMetricsLabel(), 1);
            verify(handle).release(false);
        });
        verify(mockMetrics, never()).incrementPartialBatchRejectedSignalCount(anyString(), anyLong());
    }

    @Test
    void testSend_successfulResponseWithEmptyBody_incrementsRecordsOut() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.empty()));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRecordsOut(1);
            verify(handle).release(true);
        });
    }

    @Test
    void testSend_withAdditionalHeaders_includesThemInRequest() throws Exception {
        final Map<String, String> additionalHeaders = Map.of("x-custom", "value1");
        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient, additionalHeaders, false);

        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.empty()));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            final ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(mockWebClient).execute(requestCaptor.capture());
            assertEquals("value1", requestCaptor.getValue().headers().get("x-custom"));
        });
    }

    @Test
    void testSend_withNullAdditionalHeaders_usesEmptyMap() throws Exception {
        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient, null, false);

        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.OK), HttpData.empty()));

        when(mockHandler.parsePartialSuccess(any())).thenReturn(Pair.of(0L, ""));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() ->
            verify(mockMetrics).incrementRecordsOut(1)
        );
    }

    @Test
    void testSend_redirectStatusCode_treatedAsError() throws Exception {
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(ResourceSpans.getDefaultInstance(), handle));

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        // Status 301 - not in 2xx range
        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(HttpStatus.MOVED_PERMANENTLY), HttpData.empty()));

        sender.send(batch, mockHandler, OtlpSignalType.TRACE);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRejectedRecordsCount(1);
            verify(handle).release(false);
        });
    }

    private void sendWithResponse(final boolean dropTransactionSearchErrors, final HttpStatus status,
                                  final byte[] body, final OtlpSignalType signalType, final EventHandle... handles) {
        final List<Pair<ResourceSpans, EventHandle>> batch = new ArrayList<>();
        for (final EventHandle handle : handles) {
            batch.add(Pair.of(ResourceSpans.getDefaultInstance(), handle));
        }

        setupCompressorAndSigner(new byte[]{1, 2, 3}, new byte[]{4, 5});

        when(mockWebClient.execute(any(HttpRequest.class)))
                .thenReturn(HttpResponse.of(ResponseHeaders.of(status), HttpData.wrap(body)));

        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient, Map.of(), dropTransactionSearchErrors);

        sender.send(batch, mockHandler, signalType);
    }

    private void assertRejectedNotDropped(final EventHandle handle, final OtlpSignalType signalType) {
        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementRejectedRecordsCount(1);
            verify(mockMetrics).incrementRejectedSignalCount(signalType.getMetricsLabel(), 1);
            verify(mockMetrics).incrementCompleteBatchRejectedSignalCount(signalType.getMetricsLabel(), 1);
            verify(handle).release(false);
        });
        verify(mockMetrics, never()).incrementDroppedSpansTransactionSearchDisabledCount(anyLong());
        verify(mockMetrics, never()).incrementPartialBatchRejectedSignalCount(anyString(), anyLong());
    }

    @Test
    void send_traceWith400TransactionSearchDisabledBody_dropsBatchAndAcknowledges() {
        final EventHandle handle1 = mock(EventHandle.class);
        final EventHandle handle2 = mock(EventHandle.class);

        sendWithResponse(true, HttpStatus.BAD_REQUEST, TRANSACTION_SEARCH_DISABLED_BODY, OtlpSignalType.TRACE, handle1, handle2);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementDroppedSpansTransactionSearchDisabledCount(2);
            verify(handle1).release(true);
            verify(handle2).release(true);
        });
        verify(mockMetrics).recordResponseCode(400);
        verify(mockMetrics, never()).incrementRejectedRecordsCount(anyLong());
        verify(mockMetrics, never()).incrementRejectedSignalCount(anyString(), anyLong());
        verify(mockMetrics, never()).incrementCompleteBatchRejectedSignalCount(anyString(), anyLong());
        verify(mockMetrics, never()).incrementRecordsOut(anyLong());
        verify(handle1, never()).release(false);
        verify(handle2, never()).release(false);
    }

    @Test
    void send_traceWith400TransactionSearchDisabledMessageInDifferentCase_dropsBatch() {
        final EventHandle handle = mock(EventHandle.class);
        final byte[] body = TRANSACTION_SEARCH_DISABLED_MESSAGE.toUpperCase(Locale.ROOT).getBytes(UTF_8);

        sendWithResponse(true, HttpStatus.BAD_REQUEST, body, OtlpSignalType.TRACE, handle);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(mockMetrics).incrementDroppedSpansTransactionSearchDisabledCount(1);
            verify(handle).release(true);
        });
        verify(mockMetrics, never()).incrementRejectedRecordsCount(anyLong());
    }

    @Test
    void send_traceWith400TransactionSearchDisabledBodyAndDropDisabled_rejectsBatch() {
        final EventHandle handle = mock(EventHandle.class);

        sendWithResponse(false, HttpStatus.BAD_REQUEST, TRANSACTION_SEARCH_DISABLED_BODY, OtlpSignalType.TRACE, handle);

        assertRejectedNotDropped(handle, OtlpSignalType.TRACE);
        verify(handle, never()).release(true);
    }

    @Test
    void send_traceWith400OtherBody_rejectsBatch() {
        final EventHandle handle = mock(EventHandle.class);

        sendWithResponse(true, HttpStatus.BAD_REQUEST, "Invalid span".getBytes(UTF_8), OtlpSignalType.TRACE, handle);

        assertRejectedNotDropped(handle, OtlpSignalType.TRACE);
    }

    @Test
    void send_traceWithNon400StatusAndTransactionSearchDisabledBody_rejectsBatch() {
        final EventHandle handle = mock(EventHandle.class);

        sendWithResponse(true, HttpStatus.FORBIDDEN, TRANSACTION_SEARCH_DISABLED_BODY, OtlpSignalType.TRACE, handle);

        assertRejectedNotDropped(handle, OtlpSignalType.TRACE);
    }

    @Test
    void send_logWith400TransactionSearchDisabledBody_rejectsBatch() {
        final EventHandle handle = mock(EventHandle.class);

        sendWithResponse(true, HttpStatus.BAD_REQUEST, TRANSACTION_SEARCH_DISABLED_BODY, OtlpSignalType.LOG, handle);

        assertRejectedNotDropped(handle, OtlpSignalType.LOG);
    }

    @Test
    void send_metricWith400TransactionSearchDisabledBody_rejectsBatch() {
        final EventHandle handle = mock(EventHandle.class);

        sendWithResponse(true, HttpStatus.BAD_REQUEST, TRANSACTION_SEARCH_DISABLED_BODY, OtlpSignalType.METRIC, handle);

        assertRejectedNotDropped(handle, OtlpSignalType.METRIC);
    }
}
