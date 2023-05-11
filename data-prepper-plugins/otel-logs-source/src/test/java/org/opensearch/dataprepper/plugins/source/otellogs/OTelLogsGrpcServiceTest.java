/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.InstrumentationLibraryLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class OTelLogsGrpcServiceTest {
    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder()
                    .addInstrumentationLibraryLogs(InstrumentationLibraryLogs.newBuilder()
                            .addLogRecords(LogRecord.newBuilder())
                    .build())).build();

    private static PluginSetting pluginSetting;
    private final int bufferWriteTimeoutInMillis = 100000;

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

    @Mock
    OTelProtoCodec.OTelProtoDecoder mockOTelProtoDecoder;
    @Mock
    PluginMetrics mockPluginMetrics;

    @Captor
    ArgumentCaptor<Record> recordCaptor;

    @Captor
    ArgumentCaptor<Collection<Record<OpenTelemetryLog>>> recordsCaptor;

    @Captor
    ArgumentCaptor<StatusException> statusExceptionArgumentCaptor;

    private OTelLogsGrpcService objectUnderTest;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelLogsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUEST_TIMEOUTS)).thenReturn(timeoutCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(mockPluginMetrics.summary(OTelLogsGrpcService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(mockPluginMetrics.timer(OTelLogsGrpcService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(requestProcessDuration).record(ArgumentMatchers.<Runnable>any());
    }

    @Test
    public void export_responseObserverOnCompleted() throws Exception {
        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoCodec.OTelProtoDecoder());
        objectUnderTest.export(LOGS_REQUEST, responseObserver);

        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportLogsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        verifyNoInteractions(badRequestsCounter);
        verifyNoInteractions(requestsTooLargeCounter);
        verifyNoInteractions(timeoutCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final List<Record<OpenTelemetryLog>> capturedRecords = (List<Record<OpenTelemetryLog>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
    }
    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoCodec.OTelProtoDecoder());
        doThrow(new TimeoutException()).when(buffer).writeAll(any(Collection.class), anyInt());

        objectUnderTest.export(LOGS_REQUEST, responseObserver);

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
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.RESOURCE_EXHAUSTED.getCode()));
    }

    @Test
    public void export_BadRequest_responseObserverOnError() {
        final String testMessage = "test message";
        final RuntimeException testException = new RuntimeException(testMessage);
        when(mockOTelProtoDecoder.parseExportLogsServiceRequest(any())).thenThrow(testException);
        objectUnderTest = generateOTelLogsGrpcService(mockOTelProtoDecoder);
        objectUnderTest.export(LOGS_REQUEST, responseObserver);

        verifyNoInteractions(buffer);
        verify(responseObserver, times(0)).onNext(ExportLogsServiceResponse.newBuilder().build());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(statusExceptionArgumentCaptor.capture());
        verify(requestsReceivedCounter, times(1)).increment();
        verify(badRequestsCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        verifyNoInteractions(requestsTooLargeCounter);
        verifyNoInteractions(timeoutCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.INVALID_ARGUMENT.getCode()));
    }

    @Test
    public void export_RequestTooLarge_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new SizeOverflowException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoCodec.OTelProtoDecoder());
        objectUnderTest.export(LOGS_REQUEST, responseObserver);

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
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.RESOURCE_EXHAUSTED.getCode()));
    }

    @Test
    public void export_BufferInternalException_responseObserverOnError() throws Exception {
        final String testMessage = "test message";
        doThrow(new IOException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoCodec.OTelProtoDecoder());
        objectUnderTest.export(LOGS_REQUEST, responseObserver);

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
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
        StatusException capturedStatusException = statusExceptionArgumentCaptor.getValue();
        assertThat(capturedStatusException.getStatus().getCode(), equalTo(Status.INTERNAL.getCode()));
    }


    private OTelLogsGrpcService generateOTelLogsGrpcService(final OTelProtoCodec.OTelProtoDecoder decoder) {
        return new OTelLogsGrpcService(
                bufferWriteTimeoutInMillis, decoder, buffer, mockPluginMetrics);
    }
}
