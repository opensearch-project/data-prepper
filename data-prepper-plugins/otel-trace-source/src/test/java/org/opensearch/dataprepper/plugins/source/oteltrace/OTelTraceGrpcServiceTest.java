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
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.common.v1.InstrumentationScope;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.skyscreamer.jsonassert.JSONAssert;
import io.micrometer.core.instrument.util.IOUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
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
    private static final int TIME_DELTA = 1234;
    private static final int TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT = 11;
    private static final int TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT = 22;
    private static final String IS_SCOPE_NAME="testISScopeName";
    private static final String IS_SCOPE_VERSION="testISScopeVersion";
    private static final String IS_SCOPE_ATTR_KEY="scope.attr";
    private static final String IS_SCOPE_ATTR_VALUE="testISScopeAttrValue";
    private static final String TRACE_SERVICE_NAME = "TestTraceServiceName";
    private static final String RESOURCE_ATTR_SERVICE_KEY = "service.name";
    private static final int SPAN_DROPPED_ATTR_COUNT = 10;
    private static final String TEST_SCHEMA_URL="testSchemaURL";
    private static final String TEST_TRACE_ID = "testTraceID";
    private static final String TEST_SPAN_ID = "testSpanID";
    private static final String TEST_REGION = "testRegion";
    private static final String TEST_SCOPE_SPAN_NAME = "testScopeSpanName";
    private static final String TEST_NAME_SPACE = "testNameSpace";
    private static final String NAME_SPACE_KEY = "nameSpace";
    private static final String REGION_KEY = "region";
    private static final String DATA_KEY = "data";

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

    private String TIME_KEY;
    private String END_TIME_KEY;
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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_Success_responseObserverOnCompleted(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        objectUnderTest = generateOTelTraceGrpcService(decoder);

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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_Success_with_ByteBuffer_responseObserverO(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        objectUnderTest = generateOTelTraceGrpcService(decoder);

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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_BufferTimeout_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        objectUnderTest = generateOTelTraceGrpcService(decoder);
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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_RequestTooLarge_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        final String testMessage = "test message";
        doThrow(new SizeOverflowException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelTraceGrpcService(decoder);

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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_BufferInternalException_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        final String testMessage = "test message";
        doThrow(new IOException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelTraceGrpcService(decoder);

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
    public void test_TraceSource_output_with_OpensearchFormat() throws Exception {
        final ExportTraceServiceRequest TRACE_FULL_REQUEST = createFullTraceRequest();

        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoOpensearchCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(TRACE_FULL_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        final List<Record<Span>> capturedRecords = (List<Record<Span>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        Record<Span> capturedRecord = capturedRecords.get(0);
        String result = capturedRecord.getData().toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-trace.json"));
        String expected = String.format(file, END_TIME_KEY, TIME_KEY, END_TIME_KEY);
        JSONAssert.assertEquals(expected, result, false);
    }

    @Test
    public void test_TraceSource_output_with_StandardFormat() throws Exception {
        final ExportTraceServiceRequest TRACE_FULL_REQUEST = createFullTraceRequest();

        objectUnderTest = generateOTelTraceGrpcService(new OTelProtoStandardCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(TRACE_FULL_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        final List<Record<Span>> capturedRecords = (List<Record<Span>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        Record<Span> capturedRecord = capturedRecords.get(0);
        String result = capturedRecord.getData().toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-standard-trace.json"));
        String expected = String.format(file, TIME_KEY, END_TIME_KEY);
        JSONAssert.assertEquals(expected, result, false);
    }


    private OTelTraceGrpcService generateOTelTraceGrpcService(final OTelProtoCodec.OTelProtoDecoder decoder) {
        return new OTelTraceGrpcService(
                bufferWriteTimeoutInMillis, decoder, buffer, mockPluginMetrics);
    }

    private ExportTraceServiceRequest createFullTraceRequest() {

        Instant currentTime = Instant.now();
        long currentUnixTimeNano = ((long)currentTime.getEpochSecond() * 1000_000_000L) + currentTime.getNano();
        Instant endTime = currentTime.plusSeconds(TIME_DELTA);
        TIME_KEY = currentTime.toString();
        END_TIME_KEY = endTime.toString();
        long endUnixTimeNano = currentUnixTimeNano + (TIME_DELTA*1000_000_000L);

        final Resource resource = Resource.newBuilder()
                .setDroppedAttributesCount(TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey(RESOURCE_ATTR_SERVICE_KEY)
                        .setValue(AnyValue.newBuilder().setStringValue(TRACE_SERVICE_NAME).build())
                )
                .build();

        final InstrumentationScope instrumentationScope = InstrumentationScope.newBuilder()
                .setName(IS_SCOPE_NAME)
                .setVersion(IS_SCOPE_VERSION)
                .setDroppedAttributesCount(TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey(IS_SCOPE_ATTR_KEY)
                        .setValue(AnyValue.newBuilder().setStringValue(IS_SCOPE_ATTR_VALUE).build())
                ).build();

        KeyValue setValue = KeyValue.newBuilder().setKey(NAME_SPACE_KEY).
                                    setValue(AnyValue.newBuilder().setStringValue(TEST_NAME_SPACE).build()).build();
        KeyValueList kvList = KeyValueList.newBuilder().addValues(setValue).build();
        final ScopeSpans scopeSpans = ScopeSpans.newBuilder()
                .setScope(instrumentationScope)
                .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder()
                            .setTraceId(ByteString.copyFrom(TEST_TRACE_ID.getBytes()))
                            .setSpanId(ByteString.copyFrom(TEST_SPAN_ID.getBytes()))
                            .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_INTERNAL)
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(REGION_KEY)
                                .setValue(AnyValue.newBuilder().setStringValue(TEST_REGION).build()).build())
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(DATA_KEY)
                                .setValue(AnyValue.newBuilder().setKvlistValue(kvList).build()).build())
                            .setName(TEST_SCOPE_SPAN_NAME)
                            .setDroppedAttributesCount(SPAN_DROPPED_ATTR_COUNT)
                            .setEndTimeUnixNano(endUnixTimeNano)
                            .build())
                .build();

        ResourceSpans resourceSpans = ResourceSpans.newBuilder()
                .setResource(resource)
                .addScopeSpans(scopeSpans)
                .setSchemaUrl(TEST_SCHEMA_URL)
                .build();

        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(resourceSpans)
                .build();
    }

    private static Stream<Arguments> getDecoderArguments() {
        return Stream.of(
            Arguments.of(new OTelProtoOpensearchCodec.OTelProtoDecoder()),
            Arguments.of(new OTelProtoStandardCodec.OTelProtoDecoder())
        );
    }
}
