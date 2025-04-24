/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.google.protobuf.ByteString;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
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
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class OTelLogsGrpcServiceTest {
    private static final int TIME_DELTA = 1234;
    private static final int TEST_SEVERITY_NUMBER = 2;
    private static final String TEST_SEVERITY_TEXT = "Test severity text";
    private static final int TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT = 11;
    private static final int TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT = 22;
    private static final String TEST_BODY = "Test body";
    private static final int TEST_DROPPED_ATTRIBUTES_COUNT = 10;
    private static final String TEST_TRACE_ID = "testTraceId";
    private static final String TEST_SPAN_ID = "testSpanId";
    private static final String TEST_REGION = "testRegion";
    private static final String TEST_SCHEMA_URL = "testSchemaUrl";
    private static final String TEST_SERVICE_NAME = "testServiceName";
    private static final String TEST_SCOPE_ATTR = "testScopeAttr";
    private static final String TEST_SCOPE_NAME = "testScopeName";
    private static final String TEST_SCOPE_VERSION = "testScopeVersion";
    private static final String TEST_NAME_SPACE = "testNameSpace";
    private static final String NAME_SPACE_KEY = "nameSpace";
    private static final String REGION_KEY = "region";
    private static final String DATA_KEY = "data";
    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder()
                    .addScopeLogs(ScopeLogs.newBuilder().addLogRecords(LogRecord.newBuilder()) .build()))
            .build();


    private static PluginSetting pluginSetting;
    private final int bufferWriteTimeoutInMillis = 100000;

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
    OTelProtoCodec.OTelProtoDecoder mockOTelProtoDecoder;
    @Mock
    PluginMetrics mockPluginMetrics;
    @Mock
    private ServiceRequestContext serviceRequestContext;

    @Captor
    ArgumentCaptor<Record> recordCaptor;

    @Captor
    ArgumentCaptor<Collection<Record<OpenTelemetryLog>>> recordsCaptor;

    @Captor
    ArgumentCaptor<byte[]> bytesCaptor;

    private OTelLogsGrpcService objectUnderTest;
    private String TIME_KEY;
    private String OBSERVED_TIME_KEY;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelLogsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        mockPluginMetrics = mock(PluginMetrics.class);

        when(mockPluginMetrics.counter(OTelLogsGrpcService.REQUESTS_RECEIVED)).thenReturn(requestsReceivedCounter);
        when(mockPluginMetrics.counter(OTelLogsGrpcService.SUCCESS_REQUESTS)).thenReturn(successRequestsCounter);
        when(mockPluginMetrics.summary(OTelLogsGrpcService.PAYLOAD_SIZE)).thenReturn(payloadSizeSummary);
        when(mockPluginMetrics.timer(OTelLogsGrpcService.REQUEST_PROCESS_DURATION)).thenReturn(requestProcessDuration);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(requestProcessDuration).record(ArgumentMatchers.<Runnable>any());

        when(serviceRequestContext.isTimedOut()).thenReturn(false);
    }

    private ExportLogsServiceRequest createLogRequest() {

        final Instant currentTime = Instant.now();
        final long currentUnixTimeNano = ((long)currentTime.getEpochSecond() * 1000_000_000L) + currentTime.getNano();
        final long observedUnixTimeNano = currentUnixTimeNano + (TIME_DELTA*1000_000_000L);
        final Instant observedTime = currentTime.plusSeconds(TIME_DELTA);
        TIME_KEY = currentTime.toString();
        OBSERVED_TIME_KEY = observedTime.toString();
        final Resource resource = Resource.newBuilder()
                .setDroppedAttributesCount(TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue(TEST_SERVICE_NAME).build())
                ).build();
        final InstrumentationScope instrumentationScope = InstrumentationScope.newBuilder()
                .setName(TEST_SCOPE_NAME)
                .setVersion(TEST_SCOPE_VERSION)
                .setDroppedAttributesCount(TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey("scope.attr")
                        .setValue(AnyValue.newBuilder().setStringValue(TEST_SCOPE_ATTR).build())
                ).build();

        KeyValue setValue = KeyValue.newBuilder().setKey(NAME_SPACE_KEY).
                                    setValue(AnyValue.newBuilder().setStringValue(TEST_NAME_SPACE).build()).build();
        KeyValueList kvList = KeyValueList.newBuilder().addValues(setValue).build();
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .addLogRecords(LogRecord.newBuilder()
                            .setTimeUnixNano(currentUnixTimeNano)
                            .setObservedTimeUnixNano(observedUnixTimeNano)
                            .setSeverityNumberValue(TEST_SEVERITY_NUMBER)
                            .setSeverityText(TEST_SEVERITY_TEXT)
                            .setBody(AnyValue.newBuilder().setStringValue(TEST_BODY).build())
                            .setDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                            .setTraceId(ByteString.copyFrom(TEST_TRACE_ID.getBytes()))
                            .setSpanId(ByteString.copyFrom(TEST_SPAN_ID.getBytes()))
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(REGION_KEY)
                                .setValue(AnyValue.newBuilder().setStringValue(TEST_REGION).build()).build())
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(DATA_KEY)
                                .setValue(AnyValue.newBuilder().setKvlistValue(kvList).build()).build())
                            )
                        .setSchemaUrl(TEST_SCHEMA_URL)
                        .setScope(instrumentationScope)
                        )
                .setResource(resource)
                .setSchemaUrl(TEST_SCHEMA_URL)
                .build())
            .build();

    }

    @Test
    public void test_LogsSource_output_with_OpensearchFormat() throws Exception {
        final ExportLogsServiceRequest LOGS_FULL_REQUEST = createLogRequest();

        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoOpensearchCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(LOGS_FULL_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        final List<Record<OpenTelemetryLog>> capturedRecords = (List<Record<OpenTelemetryLog>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        Record<OpenTelemetryLog> capturedRecord = capturedRecords.get(0);
        String result = capturedRecord.getData().toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-log.json"));
        String expected = String.format(file, TIME_KEY, OBSERVED_TIME_KEY);
        JSONAssert.assertEquals(expected, result, false);
    }

    @Test
    public void test_LogsSource_output_with_StandardFormat() throws Exception {
        final ExportLogsServiceRequest LOGS_FULL_REQUEST = createLogRequest();

        objectUnderTest = generateOTelLogsGrpcService(new OTelProtoStandardCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(LOGS_FULL_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        final List<Record<OpenTelemetryLog>> capturedRecords = (List<Record<OpenTelemetryLog>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
        Record<OpenTelemetryLog> capturedRecord = capturedRecords.get(0);
        String result = capturedRecord.getData().toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-standard-log.json"));
        String expected = String.format(file, TIME_KEY, OBSERVED_TIME_KEY);
        JSONAssert.assertEquals(expected, result, false);

    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_responseObserverOnCompleted(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        objectUnderTest = generateOTelLogsGrpcService(decoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(LOGS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeAll(recordsCaptor.capture(), anyInt());
        verify(responseObserver, times(1)).onNext(ExportLogsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final List<Record<OpenTelemetryLog>> capturedRecords = (List<Record<OpenTelemetryLog>>) recordsCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(1));
    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_with_ByteBuffer_responseObserverOnCompleted(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        objectUnderTest = generateOTelLogsGrpcService(decoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            objectUnderTest.export(LOGS_REQUEST, responseObserver);
        }

        verify(buffer, times(1)).writeBytes(bytesCaptor.capture(), eq(null), anyInt());
        verify(responseObserver, times(1)).onNext(ExportLogsServiceResponse.newBuilder().build());
        verify(responseObserver, times(1)).onCompleted();
        verify(requestsReceivedCounter, times(1)).increment();
        verify(successRequestsCounter, times(1)).increment();
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());

        final byte[] capturedBytes = (byte[]) bytesCaptor.getValue();
        assertThat(capturedBytes.length, equalTo(LOGS_REQUEST.toByteArray().length));
    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_BufferTimeout_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        objectUnderTest = generateOTelLogsGrpcService(decoder);
        doThrow(new TimeoutException()).when(buffer).writeAll(any(Collection.class), anyInt());

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(LOGS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @Test
    public void export_BadRequest_responseObserverOnError() {
        final String testMessage = "test message";
        final RuntimeException testException = new RuntimeException(testMessage);
        when(mockOTelProtoDecoder.parseExportLogsServiceRequest(any(), any(Instant.class))).thenThrow(testException);
        objectUnderTest = generateOTelLogsGrpcService(mockOTelProtoDecoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BadRequestException.class, () -> objectUnderTest.export(LOGS_REQUEST, responseObserver));
        }

        verifyNoInteractions(buffer);
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_RequestTooLarge_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        final String testMessage = "test message";
        doThrow(new SizeOverflowException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelLogsGrpcService(decoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(LOGS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_BufferInternalException_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        final String testMessage = "test message";
        doThrow(new IOException(testMessage)).when(buffer).writeAll(any(Collection.class), anyInt());
        objectUnderTest = generateOTelLogsGrpcService(decoder);

        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            assertThrows(BufferWriteException.class, () -> objectUnderTest.export(LOGS_REQUEST, responseObserver));
        }

        verify(buffer, times(1)).writeAll(any(Collection.class), anyInt());
        verifyNoInteractions(responseObserver);
        verify(requestsReceivedCounter, times(1)).increment();
        verifyNoInteractions(successRequestsCounter);
        final ArgumentCaptor<Double> payloadLengthCaptor = ArgumentCaptor.forClass(Double.class);
        verify(payloadSizeSummary, times(1)).record(payloadLengthCaptor.capture());
        assertThat(payloadLengthCaptor.getValue().intValue(), equalTo(LOGS_REQUEST.getSerializedSize()));
        verify(requestProcessDuration, times(1)).record(ArgumentMatchers.<Runnable>any());
    }


    private OTelLogsGrpcService generateOTelLogsGrpcService(final OTelProtoCodec.OTelProtoDecoder decoder) {
        return new OTelLogsGrpcService(
                bufferWriteTimeoutInMillis, decoder, buffer, mockPluginMetrics);
    }

    private static Stream<Arguments> getDecoderArguments() {
        return Stream.of(
            Arguments.of(new OTelProtoOpensearchCodec.OTelProtoDecoder()),
            Arguments.of(new OTelProtoStandardCodec.OTelProtoDecoder())
        );
    }
}
