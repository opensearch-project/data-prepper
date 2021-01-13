package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


public class OTelTraceGrpcServiceTest {

    private OTelTraceGrpcService oTelTraceGrpcService;
    private static PluginSetting pluginSetting;
    private final int bufferWriteTimeoutInMillis = 100000;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelTraceGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");
    }

    @Test
    public void testRequestsReceivedCounter() {

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);

        ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder().build();
        StreamObserver response = mock(StreamObserver.class);
        Counter mockRequestsReceivedCounter = mock(Counter.class);
        Counter mockTimeoutCounter = mock(Counter.class);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_RECEIVED)).thenReturn(mockRequestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUEST_TIMEOUTS)).thenReturn(mockTimeoutCounter);

        oTelTraceGrpcService = new OTelTraceGrpcService(bufferWriteTimeoutInMillis, mock(Buffer.class), mockPluginMetrics);
        oTelTraceGrpcService.export(request, response);

        verify(response, times(1)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(response, times(1)).onCompleted();
        verify(mockRequestsReceivedCounter, times(1)).increment();
        verifyZeroInteractions(mockTimeoutCounter);
    }

    @Test
    public void testTimeoutCounter() throws TimeoutException {

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);
        Buffer mockBuffer = mock(Buffer.class);
        ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder().build();
        StreamObserver response = mock(StreamObserver.class);
        Counter mockRequestsReceivedCounter = mock(Counter.class);
        Counter mockTimeoutCounter = mock(Counter.class);
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUESTS_RECEIVED)).thenReturn(mockRequestsReceivedCounter);
        doThrow(new TimeoutException()).when(mockBuffer).write(any(Record.class), anyInt());
        when(mockPluginMetrics.counter(OTelTraceGrpcService.REQUEST_TIMEOUTS)).thenReturn(mockTimeoutCounter);

        oTelTraceGrpcService = new OTelTraceGrpcService(bufferWriteTimeoutInMillis, mockBuffer, mockPluginMetrics);
        oTelTraceGrpcService.export(request, response);

        verify(response, times(0)).onNext(ExportTraceServiceResponse.newBuilder().build());
        verify(response, times(0)).onCompleted();
        verify(mockTimeoutCounter, times(1)).increment();
        verify(mockRequestsReceivedCounter, times(1)).increment();
    }
}
