/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

@ExtendWith(MockitoExtension.class)
public class OTelMetricsGrpcServiceTest {
    private static NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
    private static Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();
    private static final ExportMetricsServiceRequest METRICS_REQUEST = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder()
                    .addScopeMetrics(ScopeMetrics.newBuilder()
                            .addMetrics(Metric.newBuilder().setGauge(gauge).setUnit("seconds").setName("name").build())
                    .build())).build();

    private static Map<String, Object> expectedMetric = Map.of("unit", (Object)"seconds", "name", (Object)"name", "kind", (Object)"GAUGE");
    private static PluginSetting pluginSetting;
    private final int bufferWriteTimeoutInMillis = 100000;

    @Mock
    private Counter requestsReceivedCounter;
    @Mock
    private Counter successRequestsCounter;
    @Mock
    private Counter droppedCounter;
    @Mock
    private Counter createdCounter;
    @Mock
    private DistributionSummary payloadSize;
    @Mock
    private Timer requestProcessDuration;
    @Mock
    private StreamObserver responseObserver;
    @Mock
    private Buffer buffer;
    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Captor
    private ArgumentCaptor<Collection<Record>> recordCaptor;

    @Captor
    ArgumentCaptor<byte[]> bytesCaptor;

    private OTelMetricsGrpcService sut;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelMetricsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelMetricsGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelMetricsGrpcService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(mockPluginMetrics.counter(OTelMetricsGrpcService.RECORDS_CREATED)).thenReturn(createdCounter);
        when(mockPluginMetrics.counter(OTelMetricsGrpcService.RECORDS_DROPPED)).thenReturn(droppedCounter);
        when(mockPluginMetrics.summary(OTelMetricsGrpcService.PAYLOAD_SIZE)).thenReturn(payloadSize);
        when(mockPluginMetrics.timer(OTelMetricsGrpcService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(requestProcessDuration).record(ArgumentMatchers.<Runnable>any());

        when(serviceRequestContext.isTimedOut()).thenReturn(false);

        sut = new OTelMetricsGrpcService(bufferWriteTimeoutInMillis, new OTelProtoCodec.OTelProtoDecoder(), buffer, mockPluginMetrics);
    }

    @Test
    public void export_Success_responseObserverOnCompleted() throws Exception {
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            sut.export(METRICS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeAll(recordCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportMetricsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();

        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSize, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(METRICS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        Collection<Record> capturedRecords = recordCaptor.getValue();
        Record capturedRecord = (Record)(capturedRecords.toArray()[0]);
        Map<String, Object> map = ((Event)capturedRecord.getData()).toMap();

        expectedMetric.forEach((k, v) -> assertThat(map, hasEntry((String)k, (Object)v)));
    }

    @Test
    public void export_Success_with_ByteBuffer_responseObserverOnCompleted() throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            sut.export(METRICS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeBytes(bytesCaptor.capture(), eq(null), anyInt());
        verify(responseObserver, times(1)).onNext(ExportMetricsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();

        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSize, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(METRICS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final byte[] capturedBytes = (byte[]) bytesCaptor.getValue();
        assertThat(capturedBytes.length, equalTo(METRICS_REQUEST.toByteArray().length));
    }

    @Test
    public void export_BufferTimeout_responseObserverOnError() throws Exception {
        doThrow(new TimeoutException()).when(buffer).writeAll(any(Collection.class), anyInt());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> sut.export(METRICS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);

        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSize, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(METRICS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }
}
