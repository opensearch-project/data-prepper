/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.google.protobuf.ByteString;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
                    .addScopeSpans(ScopeSpans.newBuilder().addSpans(TEST_SPAN)).build())
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
    StreamObserver responseObserver;
    @Mock
    Buffer buffer;
    @Mock
    Counter successRequestsCounter;
    @Mock
    DistributionSummary payloadSizeSummary;
    @Mock
    Timer requestProcessDuration;
    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Captor
    ArgumentCaptor<byte[]> bytesCaptor;

    @Captor
    ArgumentCaptor<Record> recordCaptor;

    @Captor
    ArgumentCaptor<Collection<Record<Span>>> recordsCaptor;

    @Captor
    ArgumentCaptor<StatusException> statusExceptionArgumentCaptor;

    private OTelTraceGrpcService objectUnderTest;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelTraceGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(mockPluginMetrics.summary(OTelTraceGrpcService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(mockPluginMetrics.timer(OTelTraceGrpcService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(requestProcessDuration).record(ArgumentMatchers.<Runnable>any());

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoCodec.OTelProtoDecoder());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(SUCCESS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final List<Record<Span>> capturedRecords = (List<Record<Span>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        assertThat(capturedRecords.get(0).getData().getTraceState(), equalTo("SUCCESS"));
    }

    @Test
    public void export_Success_with_ByteBuffer_responseObserverOnCompleted() throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoCodec.OTelProtoDecoder());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(SUCCESS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeBytes(bytesCaptor.capture(), anyString(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final byte[] capturedBytes = (byte[]) bytesCaptor.getValue();
        assertThat(capturedBytes.length, equalTo(SUCCESS_REQUEST.toByteArray().length));
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoCodec.OTelProtoDecoder());
        doThrow(new TimeoutException()).when(buffer).writeAll(any(Collection.class), anyInt());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(SUCCESS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @Test
    public void export_BadRequest_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        final RuntimeException testException = new RuntimeException(testMessage);
        when(mockOTelProtoDecoder.parseExportTraceServiceRequest(any(), any(Instant.class))).thenThrow(testException);
        objectUnderTest = generateOTelTraceGrpcService(mockOTelProtoDecoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BadRequestException.class, () -> objectUnderTest.export(SUCCESS_REQUEST, responseObserver));
        }

        verifyNoInteractions(buffer);
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @Test
    public void export_RequestTooLarge_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new SizeOverflowException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoCodec.OTelProtoDecoder());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(SUCCESS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @Test
    public void export_BufferInternalException_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new IOException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoCodec.OTelProtoDecoder());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(SUCCESS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(SUCCESS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    private OTelTraceGrpcService generateOTelTraceGrpcService(final OTelProtoCodec.OTelProtoDecoder decoder) {
        return new OTelTraceGrpcService(
                bufferWriteTimeoutInMillis, decoder, buffer, mockPluginMetrics);
    }
}
