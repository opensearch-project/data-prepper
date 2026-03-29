/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.xerial.snappy.Snappy;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrometheusRemoteWriteServiceTest {

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PrometheusRemoteWriteSourceConfig config;

    @Mock
    private RemoteWriteProtobufParser protobufParser;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private Counter requestsReceivedCounter;

    @Mock
    private Counter successRequestsCounter;

    @Mock
    private Counter failedRequestsCounter;

    @Mock
    private Counter timeoutRequestsCounter;

    @Mock
    private Counter recordsCreatedCounter;

    @Mock
    private DistributionSummary payloadSizeSummary;

    @Mock
    private Timer requestProcessDuration;

    private PrometheusRemoteWriteService service;

    @BeforeEach
    void setUp() {
        lenient().when(pluginMetrics.counter(PrometheusRemoteWriteService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        lenient().when(pluginMetrics.counter(PrometheusRemoteWriteService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        lenient().when(pluginMetrics.counter(PrometheusRemoteWriteService.FAILED_REQUESTS)).thenReturn(failedRequestsCounter);
        lenient().when(pluginMetrics.counter(PrometheusRemoteWriteService.TIMEOUT_REQUESTS)).thenReturn(timeoutRequestsCounter);
        lenient().when(pluginMetrics.counter(PrometheusRemoteWriteService.RECORDS_CREATED)).thenReturn(recordsCreatedCounter);
        lenient().when(pluginMetrics.summary(PrometheusRemoteWriteService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        lenient().when(pluginMetrics.timer(PrometheusRemoteWriteService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);

        lenient().when(config.isFlattenLabels()).thenReturn(false);

        service = new PrometheusRemoteWriteService(5000, buffer, pluginMetrics,
                new RemoteWriteProtobufParser(config));
    }

    @Test
    void testDoPostWithValidRequest() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        verify(requestsReceivedCounter).increment();
        verify(payloadSizeSummary).record(anyDouble());
    }

    @Test
    void testDoPostWithTimeout() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(true);

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.REQUEST_TIMEOUT));
        verify(timeoutRequestsCounter).increment();
    }

    @Test
    void testDoPostWithEmptyBody() throws Exception {
        final AggregatedHttpRequest request = createRequest(new byte[0], MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.BAD_REQUEST));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWithInvalidProtobuf() throws Exception {
        final byte[] invalidPayload = "not valid protobuf".getBytes();
        final AggregatedHttpRequest request = createRequest(invalidPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.BAD_REQUEST));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWritesToBuffer() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.OK));
        verify(buffer).writeAll(any(), eq(5000));
        verify(successRequestsCounter).increment();
        verify(recordsCreatedCounter).increment(1);
    }

    @Test
    void testDoPostBufferWriteFailure() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        doThrow(new TimeoutException("Buffer full")).when(buffer).writeAll(any(), anyInt());

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(failedRequestsCounter).increment();
    }

    private byte[] createValidCompressedPayload() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("localhost").build())
                .addSamples(Types.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                .build();

        final Remote.WriteRequest writeRequest = Remote.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        return Snappy.compress(writeRequest.toByteArray());
    }

    private AggregatedHttpRequest createRequest(final byte[] body, final MediaType contentType) {
        final RequestHeaders headers = RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                .contentType(contentType)
                .build();

        return AggregatedHttpRequest.of(headers, HttpData.wrap(body));
    }

    private AggregatedHttpRequest createRequestWithoutContentType(final byte[] body) {
        final RequestHeaders headers = RequestHeaders.builder(HttpMethod.POST, "/api/v1/write")
                .build();

        return AggregatedHttpRequest.of(headers, HttpData.wrap(body));
    }

    @Test
    void testDoPostWithInvalidContentType() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.JSON_UTF_8);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWithTextPlainContentType() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PLAIN_TEXT_UTF_8);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWithNullContentType() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequestWithoutContentType(compressedPayload);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostEmptyWriteRequestReturns204() throws Exception {
        final Remote.WriteRequest emptyRequest = Remote.WriteRequest.newBuilder().build();
        final byte[] compressed = Snappy.compress(emptyRequest.toByteArray());
        final AggregatedHttpRequest request = createRequest(compressed, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.NO_CONTENT));
        verify(successRequestsCounter).increment();
        verify(recordsCreatedCounter, never()).increment(anyDouble());
    }

    @Test
    void testDoPostSuccessMetricsRecordedCorrectly() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        service.doPost(serviceRequestContext, request);

        verify(requestsReceivedCounter).increment();
        verify(payloadSizeSummary).record(anyDouble());
        verify(successRequestsCounter).increment();
        verify(recordsCreatedCounter).increment(1);
    }

    @Test
    void testDoPostRecordsCreatedCountMatchesRecordCount() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1706869800000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(2.0).setTimestamp(1706869860000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(3.0).setTimestamp(1706869920000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final AggregatedHttpRequest request = createRequest(compressed, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        service.doPost(serviceRequestContext, request);

        verify(recordsCreatedCounter).increment(3);
    }

    @Test
    void testDoPostBufferWriteTimeoutExceptionReturns503() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        doThrow(new TimeoutException("Buffer full")).when(buffer).writeAll(any(), anyInt());

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostPayloadSizeIsRecordedBeforeTimeout() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(true);

        service.doPost(serviceRequestContext, request);

        verify(requestsReceivedCounter).increment();
        verify(payloadSizeSummary).record(anyDouble());
        verify(timeoutRequestsCounter).increment();
    }

    @Test
    void testDoPostWithMultipleTimeSeriesWritesToBuffer() throws Exception {
        final Types.TimeSeries ts1 = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("metric_a").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1706869800000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(2.0).setTimestamp(1706869860000L).build())
                .build();

        final Types.TimeSeries ts2 = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("metric_b").build())
                .addSamples(Types.Sample.newBuilder().setValue(3.0).setTimestamp(1706869800000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(4.0).setTimestamp(1706869860000L).build())
                .build();

        final Types.TimeSeries ts3 = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("metric_c").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(6.0).setTimestamp(1706869860000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder()
                        .addTimeseries(ts1).addTimeseries(ts2).addTimeseries(ts3)
                        .build().toByteArray());
        final AggregatedHttpRequest request = createRequest(compressed, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        service.doPost(serviceRequestContext, request);

        verify(buffer).writeAll(any(), eq(5000));
        verify(recordsCreatedCounter).increment(6);
    }

    @Test
    void testDoPostWithXProtobufContentType() throws Exception {
        final byte[] compressedPayload = createValidCompressedPayload();
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.X_PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.OK));
        verify(successRequestsCounter).increment();
    }

    @Test
    void testDoPostWithSnappyCompressedInvalidProtobuf() throws Exception {
        final byte[] truncatedProtobuf = Snappy.compress(new byte[]{0x0A, 0x10, 0x01});
        final AggregatedHttpRequest request = createRequest(truncatedProtobuf, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = service.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.BAD_REQUEST));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWithPrometheusParseException() throws Exception {
        when(protobufParser.parseDecompressed(any())).thenThrow(new PrometheusParseException("Invalid protobuf"));
        final PrometheusRemoteWriteService serviceWithMockParser =
                new PrometheusRemoteWriteService(5000, buffer, pluginMetrics, protobufParser);

        final byte[] compressedPayload = Snappy.compress("some data".getBytes());
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = serviceWithMockParser.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.BAD_REQUEST));
        verify(failedRequestsCounter).increment();
    }

    @Test
    void testDoPostWithUnexpectedExceptionDuringParsing() throws Exception {
        when(protobufParser.parseDecompressed(any())).thenThrow(new RuntimeException("Unexpected error"));
        final PrometheusRemoteWriteService serviceWithMockParser =
                new PrometheusRemoteWriteService(5000, buffer, pluginMetrics, protobufParser);

        final byte[] compressedPayload = Snappy.compress("some data".getBytes());
        final AggregatedHttpRequest request = createRequest(compressedPayload, MediaType.PROTOBUF);

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
        when(requestProcessDuration.recordCallable(ArgumentMatchers.<Callable<HttpResponse>>any())).thenAnswer(invocation -> {
            final Callable<?> callable = invocation.getArgument(0);
            return callable.call();
        });

        final HttpResponse response = serviceWithMockParser.doPost(serviceRequestContext, request);

        assertThat(response.aggregate().join().status(), equalTo(HttpStatus.BAD_REQUEST));
        verify(failedRequestsCounter).increment();
    }
}
