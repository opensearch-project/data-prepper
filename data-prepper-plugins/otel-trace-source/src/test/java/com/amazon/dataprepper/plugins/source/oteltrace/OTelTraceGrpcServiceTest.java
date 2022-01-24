/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.buffer.SizeOverflowException;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.plugins.otel.codec.OTelProtoCodec;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OTelTraceGrpcServiceTest {
    private static final io.opentelemetry.proto.trace.v1.Span TEST_SPAN = io.opentelemetry.proto.trace.v1.Span.newBuilder()
            .setTraceId(ByteString.copyFromUtf8("TEST_TRACE_ID"))
            .setSpanId(ByteString.copyFromUtf8("TEST_SPAN_ID"))
            .setName("TEST_NAME")
            .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER)
            .setStartTimeUnixNano(100)
            .setEndTimeUnixNano(101)
            .setTraceState("SUCCESS").build();
    private static final ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder().addSpans(TEST_SPAN)).build())
            .build();

    private static PluginSetting pluginSetting;
    private final int bufferWriteTimeoutInMillis = 100000;

    @Mock
    OTelProtoCodec.OTelProtoDecoder mockOTelProtoDecoder;
    @Mock
    PluginMetrics mockPluginMetrics;
    @Mock
    Counter requestsReceivedCounter;
    @Mock
    Counter timeoutCounter;
    @Mock
    StreamObserver responseObserver;
    @Mock
    Buffer buffer;
    @Mock
    Counter successRequestsCounter;
    @Mock
    Counter badRequestsCounter;
    @Mock
    Counter requestsTooLargeCounter;
    @Mock
    Counter internalServerErrorCounter;
    @Mock
    DistributionSummary payloadSizeSummary;
    @Mock
    Timer requestProcessDuration;

    @Captor
    ArgumentCaptor<Collection<Record<Span>>> recordsCaptor;

    @Captor
    ArgumentCaptor<StatusException> statusExceptionArgumentCaptor;

    private OTelTraceGrpcService sut;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelTraceGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUEST_TIMEOUTS)).thenReturn(timeoutCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(mockPluginMetrics.summary(OTelTraceGrpcService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(mockPluginMetrics.timer(OTelTraceGrpcService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(requestProcessDuration).record(ArgumentMatchers.<Runnable>any());

        sut = new OTelTraceGrpcService(bufferWriteTimeoutInMillis, new OTelProtoCodec.OTelProtoDecoder(), buffer, mockPluginMetrics);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        verifyNoInteractions(badRequestsCounter);
        verifyNoInteractions(requestsTooLargeCounter);
        verifyNoInteractions(timeoutCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        List<Record<Span>> capturedRecords = (List<Record<Span>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        assertThat(capturedRecords.get(0).getData().getTraceState(), equalTo("SUCCESS"));
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        doThrow(new TimeoutException()).when(buffer).writeAll(any(Collection.class), anyInt());

        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(statusExceptionArgumentCaptor.capture());
        verify(timeoutCounter, times(1)).increment();
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        verifyNoInteractions(badRequestsCounter);
        verifyNoInteractions(requestsTooLargeCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.RESOURCE_EXHAUSTED.getCode()));
    }

    @Test
    public void export_BadRequest_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        final RuntimeException testException = new RuntimeException(testMessage);
        when(mockOTelProtoDecoder.parseExportTraceServiceRequest(any())).thenThrow(testException);
        sut = new OTelTraceGrpcService(bufferWriteTimeoutInMillis, mockOTelProtoDecoder, buffer, mockPluginMetrics);
        sut.export(SUCCESS_REQUEST, responseObserver);

        verifyNoInteractions(buffer);
        verify(responseObserver, times(0)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(statusExceptionArgumentCaptor.capture());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(badRequestsCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        verifyNoInteractions(requestsTooLargeCounter);
        verifyNoInteractions(timeoutCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.INVALID_ARGUMENT.getCode()));
    }

    @Test
    public void export_RequestTooLarge_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new SizeOverflowException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());

        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(statusExceptionArgumentCaptor.capture());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(requestsTooLargeCounter, times(1)).increment();
        verifyNoInteractions(timeoutCounter);
        verifyNoInteractions(successRequestsCounter);
        verifyNoInteractions(badRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.RESOURCE_EXHAUSTED.getCode()));
    }

    @Test
    public void export_BufferInternalException_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new IOException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());

        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(statusExceptionArgumentCaptor.capture());
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(requestsTooLargeCounter);
        verifyNoInteractions(timeoutCounter);
        verifyNoInteractions(successRequestsCounter);
        verifyNoInteractions(badRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.INTERNAL.getCode()));
    }
}
