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

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpHttpSenderTest {

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
        });
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
            verify(handle).release(false);
        });
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
        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient, additionalHeaders);

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
        sender = new OtlpHttpSender(mockMetrics, mockCompressor, mockSigner, mockWebClient, null);

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
}
