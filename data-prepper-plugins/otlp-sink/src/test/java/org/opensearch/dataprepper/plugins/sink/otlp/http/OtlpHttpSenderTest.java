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
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.utils.Pair;

import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OtlpHttpSenderTest {

    private static final byte[] PAYLOAD = "test-otlp".getBytes(StandardCharsets.UTF_8);
    private static final int SPANS_COUNT = 3;

    private OtlpSinkMetrics metrics;
    private SigV4Signer signer;
    private WebClient webClient;
    private Function<byte[], byte[]> gzipCompressor;
    private OtlpHttpSender sender;
    private AwsCredentialsSupplier mockAwsCredSupplier;
    private List<Pair<ResourceSpans, EventHandle>> testBatch;
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

        // Create test batch with ResourceSpans and EventHandles
        testBatch = Arrays.asList(
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle1),
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle2),
                Pair.of(ResourceSpans.newBuilder().build(), mockEventHandle3)
        );

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
    void testSend_emptyBatch_returnsEarly() {
        final List<Pair<ResourceSpans, EventHandle>> emptyBatch = Collections.emptyList();

        // Prevent accidental NPE if send logic changes
        when(webClient.execute(any(HttpRequest.class))).thenThrow(new AssertionError("Should not call execute"));

        sender.send(emptyBatch);

        verifyNoInteractions(metrics);
    }

    @Test
    void testSend_successfulResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.empty())
        );

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).incrementRecordsOut(SPANS_COUNT);
            verify(metrics).incrementPayloadSize(anyLong());
            verify(metrics).incrementPayloadGzipSize(PAYLOAD.length);
            verify(metrics).recordHttpLatency(anyLong());
            verify(metrics).recordResponseCode(200);

            // Verify all event handles are released with success=true
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
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

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(2);
            verify(metrics).incrementRecordsOut(SPANS_COUNT - 2);
            verify(metrics).recordResponseCode(200);

            // All handles should still be released as true (optimistic)
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_partialSuccessWithZeroRejected() {
        final ExportTraceServiceResponse proto = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(0)
                        .setErrorMessage("")
                        .build())
                .build();

        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.wrap(proto.toByteArray()))
        );

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics, never()).incrementRejectedSpansCount(anyLong());
            verify(metrics).incrementRecordsOut(SPANS_COUNT);
            verify(metrics).recordResponseCode(200);

            // All handles should be released as true
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_parseErrorOnSuccessResponse() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(200), HttpData.ofUtf8("not-protobuf"))
        );

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).incrementErrorsCount();
            verify(metrics).incrementRecordsOut(SPANS_COUNT);
            verify(metrics).recordResponseCode(200);

            // Handles should still be released as true despite parse error
            verify(mockEventHandle1).release(true);
            verify(mockEventHandle2).release(true);
            verify(mockEventHandle3).release(true);
        });
    }

    @Test
    void testSend_nonSuccessStatus() {
        when(webClient.execute(any(HttpRequest.class))).thenReturn(
                HttpResponse.of(ResponseHeaders.of(400), HttpData.ofUtf8("{\"error\":\"bad request\"}"))
        );

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(400);
            verify(metrics).incrementRejectedSpansCount(SPANS_COUNT);

            // All handles should be released with success=false
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

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).recordResponseCode(500);
            verify(metrics).incrementRejectedSpansCount(SPANS_COUNT);

            // All handles should be released with success=false
            verify(mockEventHandle1).release(false);
            verify(mockEventHandle2).release(false);
            verify(mockEventHandle3).release(false);
        });
    }

    @Test
    void testSend_skipsSendIfGzipFails() {
        sender = new OtlpHttpSender(metrics, ignored -> new byte[0], signer, webClient);

        sender.send(testBatch);

        verify(metrics).incrementFailedSpansCount(SPANS_COUNT);
        verifyNoInteractions(webClient);

        // All handles should be released with success=false
        verify(mockEventHandle1).release(false);
        verify(mockEventHandle2).release(false);
        verify(mockEventHandle3).release(false);
    }

    @Test
    void testSend_exceptionDuringSendIncrementsRejected() {
        final HttpResponse failingResponse = HttpResponse.ofFailure(new RuntimeException("send failed"));
        when(webClient.execute(any(HttpRequest.class))).thenReturn(failingResponse);

        sender.send(testBatch);

        await().untilAsserted(() -> {
            verify(metrics).incrementRejectedSpansCount(SPANS_COUNT);

            // All handles should be released with success=false
            verify(mockEventHandle1).release(false);
            verify(mockEventHandle2).release(false);
            verify(mockEventHandle3).release(false);
        });
    }

    @Test
    void testConstructor_withDefaultConfig() {
        final OtlpSinkConfig config = mock(OtlpSinkConfig.class);

        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getFlushTimeoutMillis()).thenReturn(5000L);
        when(config.getAwsRegion()).thenReturn(software.amazon.awssdk.regions.Region.US_WEST_2);

        final OtlpHttpSender defaultSender = new OtlpHttpSender(mockAwsCredSupplier, config, metrics);
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
        final OtlpHttpSender minimalSender = new OtlpHttpSender(mockAwsCredSupplier, config, metrics);
        assertNotNull(minimalSender);
    }

    @Test
    void testGetPayloadAndCompressedPayload_privateMethod() throws Exception {
        // Test the private method via reflection to ensure 100% coverage
        final Method method = OtlpHttpSender.class.getDeclaredMethod("getPayloadAndCompressedPayload", List.class);
        method.setAccessible(true);

        when(gzipCompressor.apply(any())).thenReturn("compressed".getBytes());

        @SuppressWarnings("unchecked") final Pair<byte[], byte[]> result = (Pair<byte[], byte[]>) method.invoke(sender, testBatch);

        assertNotNull(result);
        assertNotNull(result.left()); // payload
        assertNotNull(result.right()); // compressed payload
        verify(gzipCompressor).apply(any());
    }

    @Test
    void testBuildHttpRequest_privateMethod() throws Exception {
        // Test the private method via reflection
        final Method method = OtlpHttpSender.class.getDeclaredMethod("buildHttpRequest", byte[].class);
        method.setAccessible(true);

        final HttpRequest result = (HttpRequest) method.invoke(sender, PAYLOAD);

        assertNotNull(result);
        verify(signer).signRequest(PAYLOAD);
    }

    @Test
    void testHandleResponse_privateMethod_success() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleResponse", int.class, byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, 200, null, testBatch);

        verify(metrics).recordResponseCode(200);
        verify(metrics).incrementRecordsOut(SPANS_COUNT);
        verify(mockEventHandle1).release(true);
        verify(mockEventHandle2).release(true);
        verify(mockEventHandle3).release(true);
    }

    @Test
    void testHandleResponse_privateMethod_failure() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleResponse", int.class, byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, 400, "error".getBytes(), testBatch);

        verify(metrics).recordResponseCode(400);
        verify(metrics).incrementRejectedSpansCount(SPANS_COUNT);
        verify(mockEventHandle1).release(false);
        verify(mockEventHandle2).release(false);
        verify(mockEventHandle3).release(false);
    }

    @Test
    void testHandleSuccessfulResponse_privateMethod_withNullBody() throws Exception {
        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, null, testBatch);

        verify(metrics).incrementRecordsOut(SPANS_COUNT);
        verify(mockEventHandle1).release(true);
        verify(mockEventHandle2).release(true);
        verify(mockEventHandle3).release(true);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    void testHandleSuccessfulResponse_privateMethod_withoutPartialSuccess() throws Exception {
        // Create a response with no partial_success
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder().build();
        final byte[] responseBytes = response.toByteArray();

        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, responseBytes, testBatch);

        verify(metrics).incrementRecordsOut(SPANS_COUNT);
        verify(mockEventHandle1).release(true);
        verify(mockEventHandle2).release(true);
        verify(mockEventHandle3).release(true);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    void testHandleSuccessfulResponse_privateMethod_withPartialSuccess() throws Exception {
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder()
                        .setRejectedSpans(1)
                        .setErrorMessage("test error")
                        .build())
                .build();
        final byte[] responseBytes = response.toByteArray();

        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, responseBytes, testBatch);

        verify(metrics).incrementRejectedSpansCount(1);
        verify(metrics).incrementRecordsOut(SPANS_COUNT - 1);
        verify(mockEventHandle1).release(true);
        verify(mockEventHandle2).release(true);
        verify(mockEventHandle3).release(true);
    }

    @Test
    void testHandleSuccessfulResponse_privateMethod_parseException() throws Exception {
        final byte[] invalidBytes = "invalid protobuf".getBytes();

        final Method method = OtlpHttpSender.class.getDeclaredMethod("handleSuccessfulResponse", byte[].class, List.class);
        method.setAccessible(true);

        method.invoke(sender, invalidBytes, testBatch);

        verify(metrics).incrementErrorsCount();
        verify(metrics).incrementRecordsOut(SPANS_COUNT);
        verify(mockEventHandle1).release(true);
        verify(mockEventHandle2).release(true);
        verify(mockEventHandle3).release(true);
    }
}