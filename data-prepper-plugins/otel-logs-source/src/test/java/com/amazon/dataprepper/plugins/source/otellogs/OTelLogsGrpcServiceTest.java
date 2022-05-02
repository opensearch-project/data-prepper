/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otellogs;

import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.InstrumentationLibraryLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

    @Captor
    ArgumentCaptor<Record> recordCaptor;

    private OTelLogsGrpcService sut;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelLogsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUEST_TIMEOUTS)).thenReturn(timeoutCounter);

        sut = new OTelLogsGrpcService(bufferWriteTimeoutInMillis, buffer, mockPluginMetrics);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        sut.export(LOGS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(recordCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportLogsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(timeoutCounter);

        Record capturedRecord = recordCaptor.getValue();
        assertEquals(LOGS_REQUEST, capturedRecord.getData());
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        doThrow(new TimeoutException()).when(buffer).write(any(Record.class), anyInt());

        sut.export(LOGS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(any(Record.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(timeoutCounter, times(1)).increment();
        verify(requestsReceivedCounter, times(1)).increment();
    }
}
