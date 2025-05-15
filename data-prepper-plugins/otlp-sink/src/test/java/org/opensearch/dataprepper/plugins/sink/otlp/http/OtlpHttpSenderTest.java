/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.ResponseHeaders;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;

import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OtlpHttpSenderTest {

    private static final byte[] PAYLOAD = "test-otlp".getBytes(StandardCharsets.UTF_8);
    private static final int SPANS = 3;

    private OtlpSinkMetrics metrics;
    private SigV4Signer signer;
    private WebClient webClient;
    private Function<byte[], byte[]> gzipCompressor;
    private OtlpHttpSender sender;

    @BeforeEach
    void setup() {
        metrics = mock(OtlpSinkMetrics.class);
        signer = mock(SigV4Signer.class);
        webClient = mock(WebClient.class);
        gzipCompressor = mock(Function.class);

        when(gzipCompressor.apply(any())).thenReturn(PAYLOAD);
        when(signer.signRequest(any())).thenReturn(
                software.amazon.awssdk.http.SdkHttpFullRequest.builder()
                        .method(software.amazon.awssdk.http.SdkHttpMethod.POST)
                        .uri(URI.create("https://localhost/v1/traces"))
                        .putHeader("Authorization", "sig")
                        .build()
        );

        sender = new OtlpHttpSender(metrics, gzipCompressor, signer, webClient);
    }

    @Test
    void testSend_successfulResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        sender.send(PAYLOAD, SPANS);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(SPANS);
            verify(metrics).incrementPayloadSize(PAYLOAD.length);
            verify(metrics).incrementPayloadGzipSize(PAYLOAD.length);
            verify(metrics).recordHttpLatency(anyLong());
        });
    }

    @Test
    void testSend_partialSuccessResponse() {
        final ExportTraceServiceResponse proto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(2)
                        .setErrorMessage("invalid span")
                        .build())
                .build();

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.wrap(proto.toByteArray()))
        );
        sender.send(PAYLOAD, SPANS);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(2);
            verify(metrics).incrementRecordsOut(SPANS - 2);
        });
    }

    @Test
    void testSend_parseErrorOnSuccessResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.ofUtf8("not-protobuf"))
        );

        sender.send(PAYLOAD, SPANS);

        await().untilAsserted(() -> {
            verify(metrics).incrementErrorsCount();
            verify(metrics).incrementRecordsOut(SPANS);
        });
    }

    @Test
    void testSend_nonSuccessStatus() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(400), HttpData.ofUtf8("{\"error\":\"bad request\"}"))
        );

        sender.send(PAYLOAD, SPANS);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(400);
            verify(metrics).incrementRejectedSpansCount(SPANS);
        });
    }

    @Test
    void testSend_skipsSendIfGzipFails() {
        sender = new OtlpHttpSender(metrics, ignored -> new byte[0], signer, webClient);
        sender.send(PAYLOAD, SPANS);

        verify(metrics).incrementFailedSpansCount(SPANS);
        verifyNoInteractions(webClient);
    }

    @Test
    void testSend_exceptionDuringSendIncrementsRejected() {
        final HttpResponse failingResponse = HttpResponse.ofFailure(new RuntimeException("send failed"));
        when(webClient.execute(any(HttpRequest.class))).thenReturn(failingResponse);

        sender.send(PAYLOAD, SPANS);

        await().untilAsserted(() -> verify(metrics).incrementRejectedSpansCount(SPANS));
    }

    @Test
    void testConstructor_withDefaultConfig() {
        final OtlpSinkConfig config = mock(OtlpSinkConfig.class);

        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getFlushTimeoutMillis()).thenReturn(5000L);
        when(config.getAwsRegion()).thenReturn(software.amazon.awssdk.regions.Region.US_WEST_2);

        final OtlpHttpSender defaultSender = new OtlpHttpSender(config, metrics);
        assertNotNull(defaultSender);
    }

    @Test
    void testConstructor_withMinimumThresholdConfig() {
        final OtlpSinkConfig config = mock(OtlpSinkConfig.class);

        // Set all threshold values to minimum valid input
        when(config.getMaxBatchSize()).thenReturn(0L);
        when(config.getMaxRetries()).thenReturn(0);
        when(config.getFlushTimeoutMillis()).thenReturn(1L);
        when(config.getAwsRegion()).thenReturn(software.amazon.awssdk.regions.Region.US_WEST_2);

        // Should not throw or crash
        final OtlpHttpSender minimalSender = new OtlpHttpSender(config, metrics);
        assertNotNull(minimalSender);
    }

    @Test
    void testHandleSuccessfulResponse_withNullBody_incrementsRecordsOut() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, int.class);
        method.setAccessible(true);
        method.invoke(sender, null, SPANS);

        verify(metrics).incrementRecordsOut(SPANS);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    void testHandleSuccessfulResponse_withoutPartialSuccess_incrementsRecordsOut() throws Exception {
        // Create a response with no partial_success
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder().build();
        final byte[] responseBytes = response.toByteArray();

        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, int.class);
        method.setAccessible(true);
        method.invoke(sender, responseBytes, SPANS);

        verify(metrics).incrementRecordsOut(SPANS);
        verifyNoMoreInteractions(metrics);
    }
}
