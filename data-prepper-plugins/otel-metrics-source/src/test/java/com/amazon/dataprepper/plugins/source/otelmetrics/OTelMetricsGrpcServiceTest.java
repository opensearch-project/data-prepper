/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
public class OTelMetricsGrpcServiceTest {
    private static final ExportMetricsServiceRequest METRICS_REQUEST = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder()
                    .addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder()
                            .addMetrics(Metric.newBuilder().build())
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

    private OTelMetricsGrpcService sut;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelMetricsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelMetricsGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelMetricsGrpcService.REQUEST_TIMEOUTS)).thenReturn(timeoutCounter);

        sut = new OTelMetricsGrpcService(bufferWriteTimeoutInMillis, buffer, mockPluginMetrics);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        sut.export(METRICS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(recordCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportMetricsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(timeoutCounter);

        Record capturedRecord = recordCaptor.getValue();
        assertEquals(METRICS_REQUEST, capturedRecord.getData());
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        doThrow(new TimeoutException()).when(buffer).write(any(Record.class), anyInt());

        sut.export(METRICS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(any(Record.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(timeoutCounter, times(1)).increment();
        verify(requestsReceivedCounter, times(1)).increment();
    }
}
