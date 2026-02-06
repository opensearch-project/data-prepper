package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import com.google.protobuf.ByteString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.mockStatic;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;

import com.linecorp.armeria.server.ServiceRequestContext;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;

@ExtendWith(MockitoExtension.class)
class ArmeriaHttpServiceTest {
    private static final String RESOURCE_ATTR_SERVICE_KEY = "service.name";
    private static final String TRACE_SERVICE_NAME = "TestTraceServiceName";
    private static final int TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT = 11;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestsReceivedCounter;

    @Mock
    private Counter successRequestsCounter;

    @Mock
    private DistributionSummary payloadSizeSummary;

    @Mock
    private Timer requestProcessDuration;

    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Mock
    private static ServiceRequestContext staticServiceRequestContext;

    private ArmeriaHttpService armeriaHttpService;
    private List<Record<Span>> recordsReceived;

    @BeforeEach
    void setUp() {
        serviceRequestContext = mock(ServiceRequestContext.class);
        requestProcessDuration = mock(Timer.class);
        doAnswer(invocation -> {
            Runnable f = invocation.getArgument(0);
            f.run();
            return null;
        }).when(requestProcessDuration).record(any(Runnable.class));
        when(pluginMetrics.counter(ArmeriaHttpService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(pluginMetrics.counter(ArmeriaHttpService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(pluginMetrics.summary(ArmeriaHttpService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(pluginMetrics.timer(ArmeriaHttpService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);

        serviceRequestContext.push();
        lenient().when(serviceRequestContext.isTimedOut()).thenReturn(false);
    }

    @ParameterizedTest
    @EnumSource(OTelOutputFormat.class)
    void testExportTraceSuccess(OTelOutputFormat outputFormat) throws Exception {
        when(buffer.isByteBuffer()).thenReturn(false);
        doAnswer((a)-> {
            recordsReceived = ((Collection<Record<Span>>)a.getArgument(0)).stream().collect(Collectors.toList());
            return null;
        }).when(buffer).writeAll(any(), any(Integer.class));
        try (MockedStatic<ServiceRequestContext> mocked = mockStatic(ServiceRequestContext.class)) {
            mocked.when(ServiceRequestContext::current).thenReturn(staticServiceRequestContext);
            armeriaHttpService = new ArmeriaHttpService(buffer, outputFormat, pluginMetrics, 5000);
            ExportTraceServiceRequest request = createExportTraceRequest();

            ExportTraceServiceResponse response = armeriaHttpService.exportTrace(request);

            assertNotNull(response);
            verify(requestsReceivedCounter).increment();
            verify(payloadSizeSummary).record(request.getSerializedSize());
            verify(successRequestsCounter).increment();
            assertThat(recordsReceived.size(), equalTo(1));
            Span span = recordsReceived.get(0).getData();
            if (outputFormat == OTelOutputFormat.OTEL) {
                assertThat(span.get("resource/attributes/service.name", String.class), equalTo(TRACE_SERVICE_NAME));
            } else {
                assertThat(span.get("attributes/resource.attributes.service@name", String.class), equalTo(TRACE_SERVICE_NAME));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(OTelOutputFormat.class)
    void testExportTraceWithByteBuffer(OTelOutputFormat outputFormat) throws Exception {
        try (MockedStatic<ServiceRequestContext> mocked = mockStatic(ServiceRequestContext.class)) {
            mocked.when(ServiceRequestContext::current).thenReturn(staticServiceRequestContext);
            armeriaHttpService = new ArmeriaHttpService(buffer, outputFormat, pluginMetrics, 5000);
            ExportTraceServiceRequest request = ExportTraceServiceRequest.getDefaultInstance();
            when(buffer.isByteBuffer()).thenReturn(true);

            ExportTraceServiceResponse response = armeriaHttpService.exportTrace(request);

            assertNotNull(response);
            verify(requestsReceivedCounter).increment();
            verify(successRequestsCounter).increment();
        }
    }

    @ParameterizedTest
    @EnumSource(OTelOutputFormat.class)
    void testExportTraceThrowsBadRequestException(OTelOutputFormat outputFormat) {
        try (MockedStatic<ServiceRequestContext> mocked = mockStatic(ServiceRequestContext.class)) {
            mocked.when(ServiceRequestContext::current).thenReturn(staticServiceRequestContext);
            armeriaHttpService = new ArmeriaHttpService(buffer, outputFormat, pluginMetrics, 5000);
            ExportTraceServiceRequest request = mock(ExportTraceServiceRequest.class);
            when(request.getSerializedSize()).thenReturn(100);
            when(request.getResourceSpansList()).thenThrow(new RuntimeException("Parse error"));

            assertThrows(BadRequestException.class, () -> armeriaHttpService.exportTrace(request));
            verify(requestsReceivedCounter).increment();
        }
    }

    @ParameterizedTest
    @EnumSource(OTelOutputFormat.class)
    void testExportTraceThrowsBufferWriteException(OTelOutputFormat outputFormat) throws Exception {
        try (MockedStatic<ServiceRequestContext> mocked = mockStatic(ServiceRequestContext.class)) {
            mocked.when(ServiceRequestContext::current).thenReturn(staticServiceRequestContext);
            armeriaHttpService = new ArmeriaHttpService(buffer, outputFormat, pluginMetrics, 5000);
            ExportTraceServiceRequest request = ExportTraceServiceRequest.getDefaultInstance();
            when(buffer.isByteBuffer()).thenReturn(false);
            doThrow(new RuntimeException("Buffer error")).when(buffer).writeAll(any(), anyInt());

            assertThrows(BufferWriteException.class, () -> armeriaHttpService.exportTrace(request));
            verify(requestsReceivedCounter).increment();
        }
    }

    @ParameterizedTest
    @EnumSource(OTelOutputFormat.class)
    void testExportTraceWithTimeout(OTelOutputFormat outputFormat) throws Exception {
        try (MockedStatic<ServiceRequestContext> mocked = mockStatic(ServiceRequestContext.class)) {
            mocked.when(ServiceRequestContext::current).thenReturn(staticServiceRequestContext);
            armeriaHttpService = new ArmeriaHttpService(buffer, outputFormat, pluginMetrics, 5000);
            ExportTraceServiceRequest request = ExportTraceServiceRequest.getDefaultInstance();
            when(buffer.isByteBuffer()).thenReturn(false);
            //when(serviceRequestContext.isTimedOut()).thenReturn(true);

            ExportTraceServiceResponse response = armeriaHttpService.exportTrace(request);

            assertNotNull(response);
            verify(requestsReceivedCounter).increment();
        }
    }

    private ExportTraceServiceRequest createExportTraceRequest() {
        final io.opentelemetry.proto.trace.v1.Span testSpan = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setName(UUID.randomUUID().toString())
                .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER)
                .setStartTimeUnixNano(100)
                .setEndTimeUnixNano(101)
                .setTraceState("SUCCESS").build();

        final Resource resource = Resource.newBuilder()
                .setDroppedAttributesCount(TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey(RESOURCE_ATTR_SERVICE_KEY)
                        .setValue(AnyValue.newBuilder().setStringValue(TRACE_SERVICE_NAME).build())
                )
                .build();

        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .setResource(resource)
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(testSpan)).build())
                .build();
    }
}
