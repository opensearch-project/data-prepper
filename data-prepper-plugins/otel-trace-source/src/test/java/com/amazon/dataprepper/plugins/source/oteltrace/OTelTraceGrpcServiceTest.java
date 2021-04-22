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
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
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
public class OTelTraceGrpcServiceTest {
    private static final ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addInstrumentationLibrarySpans(InstrumentationLibrarySpans.newBuilder()
                            .addSpans(Span.newBuilder().setTraceState("SUCCESS").build())).build()).build();

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

    private OTelTraceGrpcService sut;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelTraceGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUEST_TIMEOUTS)).thenReturn(timeoutCounter);

        sut = new OTelTraceGrpcService(bufferWriteTimeoutInMillis, buffer, mockPluginMetrics);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(recordCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(timeoutCounter);

        Record capturedRecord = recordCaptor.getValue();
        assertEquals(SUCCESS_REQUEST, capturedRecord.getData());
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        doThrow(new TimeoutException()).when(buffer).write(any(Record.class), anyInt());

        sut.export(SUCCESS_REQUEST, responseObserver);

        verify(buffer, times(1)).write(any(Record.class), anyInt());
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(timeoutCounter, times(1)).increment();
        verify(requestsReceivedCounter, times(1)).increment();
    }
}
