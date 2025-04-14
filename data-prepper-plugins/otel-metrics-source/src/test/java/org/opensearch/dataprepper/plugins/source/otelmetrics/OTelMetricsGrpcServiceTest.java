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
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
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
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.skyscreamer.jsonassert.JSONAssert;
import io.micrometer.core.instrument.util.IOUtils;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

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
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

@ExtendWith(MockitoExtension.class)
public class OTelMetricsGrpcServiceTest {
    private static final int TIME_DELTA = 1234;
    private static final String TEST_RESOURCE_ATTR_KEY = "testResourceAttrKey";
    private static final String TEST_RESOURCE_ATTR_VALUE = "testResourceAttrValue";
    private static final int TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT = 11;
    private static final int TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT = 22;

    private static final String IS_SCOPE_NAME="testISScopeName";
    private static final String IS_SCOPE_VERSION="testISScopeVersion";
    private static final String IS_SCOPE_ATTR_KEY="scope.attr";
    private static final String IS_SCOPE_ATTR_VALUE="testISScopeAttrValue";

    private static final int TEST_COUNT = 10;
    private static final int TEST_SUM = 100;
    private static final int TEST_MIN = 5;
    private static final int TEST_MAX = 30;
    private static final int TEST_FLAGS = 1;
    private static final int TEST_SCALE = 3;
    private static final int TEST_VALUE = 20;
    private static final int POSITIVE_BUCKET_OFFSET = -1;
    private static final int NEGATIVE_BUCKET_OFFSET = 2;
    private static final int POSITIVE_BUCKET_COUNT1 = 1;
    private static final int POSITIVE_BUCKET_COUNT2 = 3;
    private static final int POSITIVE_BUCKET_COUNT3 = 5;
    private static final int NEGATIVE_BUCKET_COUNT1 = 2;
    private static final int NEGATIVE_BUCKET_COUNT2 = 4;
    private static final int NEGATIVE_BUCKET_COUNT3 = 6;
    private static double TEST_QUANTILE = 0.5;
    private static int TEST_QUANTILE_VALUE = 100;
    private static final List<Double> TEST_EXPLICIT_BOUNDS = List.of(10.0, 20.0, 30.0, 40.0);
    private static final List<Long> TEST_BUCKET_COUNTS = List.of(5L, 10L, 15L, 20L, 25L);
    private static final String TEST_SCHEMA_URL="testSchemaUrl";
    private static final String TEST_GAUGE_METRIC_NAME = "testGaugeMetricName";
    private static final String TEST_SUM_METRIC_NAME = "testSumMetricName";
    private static final String TEST_SUMMARY_METRIC_NAME = "testSummaryMetricName";
    private static final String TEST_HISTOGRAM_METRIC_NAME = "testHistogramMetricName";
    private static final String TEST_EXPONENTIAL_HISTOGRAM_METRIC_NAME = "testExponentialHistogramMetricName";
    private static final String TEST_GAUGE_ATTR_KEY = "testGaugeAttrKey";
    private static final String TEST_GAUGE_ATTR_VALUE = "testGaugeAttrValue";
    private static final String TEST_SUM_ATTR_KEY = "testSumAttrKey";
    private static final String TEST_SUM_ATTR_VALUE = "testSumAttrValue";
    private static final String TEST_SUMMARY_ATTR_KEY = "testSumamryAttrKey";
    private static final String TEST_SUMMARY_ATTR_VALUE = "testSumamryAttrValue";
    private static final String TEST_HISTOGRAM_ATTR_KEY = "testHistogramAttrKey";
    private static final String TEST_HISTOGRAM_ATTR_VALUE = "testHistogramAttrValue";
    private static final String TEST_EXPONENTIAL_HISTOGRAM_ATTR_KEY = "testExponentialHistogramAttrKey";
    private static final String TEST_EXPONENTIAL_HISTOGRAM_ATTR_VALUE = "testExponentialHistogramAttrValue";
    private static final String TEST_SUM_DESCRIPTION = "testSumDescription";
    private static final String TEST_SUMMARY_DESCRIPTION = "testSummaryDescription";
    private static final String TEST_GAUGE_DESCRIPTION = "testGaugeDescription";
    private static final String TEST_HISTOGRAM_DESCRIPTION = "testHistogramDescription";
    private static final String TEST_EXPONENTIAL_HISTOGRAM_DESCRIPTION = "testExponentialHistogramDescription";

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
    private String startTime;
    private String endTime;

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
    PluginMetrics mockPluginMetrics;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting("OTelMetricsGrpcService", Collections.EMPTY_MAP);
        pluginSetting.setPipelineName("pipeline");

        mockPluginMetrics = mock(PluginMetrics.class);

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

    }

    OTelMetricsGrpcService createObjectUnderTest(OTelProtoCodec.OTelProtoDecoder decoder) {
        return new OTelMetricsGrpcService(bufferWriteTimeoutInMillis, decoder, buffer, mockPluginMetrics);
    }

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_Success_responseObserverOnCompleted(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        sut = createObjectUnderTest(decoder);
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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_Success_with_ByteBuffer_responseObserverOnCompleted(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        sut = createObjectUnderTest(decoder);
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

    @ParameterizedTest
    @MethodSource("getDecoderArguments")
    public void export_BufferTimeout_responseObserverOnError(OTelProtoCodec.OTelProtoDecoder decoder) throws Exception {
        sut = createObjectUnderTest(decoder);
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

    @Test
    public void test_MetricsSource_output_with_OpensearchFormat() throws Exception {
        final ExportMetricsServiceRequest METRIC_REQUEST = createMetricsRequest();

        sut = createObjectUnderTest(new OTelProtoOpensearchCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            sut.export(METRIC_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordCaptor.capture(), anyInt());
        final List<Record> capturedRecords = (List<Record>) recordCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(5));
        String result = "{";
        for (int i = 0; i < capturedRecords.size(); i++) {
            Record<JacksonMetric> capturedRecord = (Record<JacksonMetric>)capturedRecords.get(i);
            JacksonMetric metric = capturedRecord.getData();
            result += "\""+metric.getKind()+"\":";
            result += metric.toJsonString();
            if (i != capturedRecords.size()-1)
                result += ",";
        }
        result += "}";
        
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-metrics.json"));
        String expected = String.format(file, startTime, endTime, startTime, endTime, startTime, endTime, startTime, endTime,startTime, endTime);
        JSONAssert.assertEquals(expected, result, false);
    }

    @Test
    public void test_MetricsSource_output_with_StandardFormat() throws Exception {
        final ExportMetricsServiceRequest METRIC_REQUEST = createMetricsRequest();

        sut = createObjectUnderTest(new OTelProtoStandardCodec.OTelProtoDecoder());
        try (MockedStatic<ServiceRequestContext> mockedStatic = mockStatic(ServiceRequestContext.class)) {
            mockedStatic.when(ServiceRequestContext::current).thenReturn(serviceRequestContext);
            sut.export(METRIC_REQUEST, responseObserver);
        }
        verify(buffer, times(1)).writeAll(recordCaptor.capture(), anyInt());
        final List<Record> capturedRecords = (List<Record>) recordCaptor.getValue();
        assertThat(capturedRecords.size(), equalTo(5));
        String result = "{";
        for (int i = 0; i < capturedRecords.size(); i++) {
            Record<JacksonMetric> capturedRecord = (Record<JacksonMetric>)capturedRecords.get(i);
            JacksonMetric metric = capturedRecord.getData();
            result += "\""+metric.getKind()+"\":";
            result += metric.toJsonString();
            if (i != capturedRecords.size()-1)
                result += ",";
        }
        result += "}";
        
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/test-standard-metrics.json"));
        String expected = String.format(file, startTime, endTime, startTime, endTime, startTime, endTime, startTime, endTime,startTime, endTime);
        JSONAssert.assertEquals(expected, result, false);
    }

    private static Stream<Arguments> getDecoderArguments() {
        return Stream.of(
            Arguments.of(new OTelProtoOpensearchCodec.OTelProtoDecoder()),
            Arguments.of(new OTelProtoStandardCodec.OTelProtoDecoder())
        );
    }

    private ExportMetricsServiceRequest createMetricsRequest() {
        Instant currentTime = Instant.now();
        startTime = currentTime.toString();
        final long currentUnixTimeNano = ((long)currentTime.getEpochSecond() * 1000_000_000L) + currentTime.getNano();
        final long endUnixTimeNano = currentUnixTimeNano + (TIME_DELTA*1000_000_000L);
        endTime = currentTime.plusSeconds(TIME_DELTA).toString();
        final Resource resource = Resource.newBuilder()
                .setDroppedAttributesCount(TEST_RESOURCE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey(TEST_RESOURCE_ATTR_KEY)
                        .setValue(AnyValue.newBuilder().setStringValue(TEST_RESOURCE_ATTR_VALUE).build())
                ).build();

        final InstrumentationScope instrumentationScope = InstrumentationScope.newBuilder()
                .setName(IS_SCOPE_NAME)
                .setVersion(IS_SCOPE_VERSION)
                .setDroppedAttributesCount(TEST_SCOPE_DROPPED_ATTRIBUTES_COUNT)
                .addAttributes(KeyValue.newBuilder()
                        .setKey(IS_SCOPE_ATTR_KEY)
                        .setValue(AnyValue.newBuilder().setStringValue(IS_SCOPE_ATTR_VALUE).build())
                ).build();
        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .setScope(instrumentationScope)
                .setSchemaUrl(TEST_SCHEMA_URL)
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName(TEST_GAUGE_METRIC_NAME)
                    .setUnit("1")
                    .setDescription(TEST_GAUGE_DESCRIPTION)
                    .setGauge(Gauge.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(TEST_GAUGE_ATTR_KEY)
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue(TEST_GAUGE_ATTR_VALUE)
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setTimeUnixNano(endUnixTimeNano)
                            .setAsInt(TEST_COUNT)
                            .build())
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName(TEST_SUM_METRIC_NAME)
                    .setDescription(TEST_SUM_DESCRIPTION)
                    .setUnit("1")
                    .setSum(Sum.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(TEST_SUM_ATTR_KEY)
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue(TEST_SUM_ATTR_VALUE)
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setTimeUnixNano(endUnixTimeNano)
                            .setAsInt(TEST_SUM)
                            .build())
                        .setAggregationTemporality(io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName(TEST_EXPONENTIAL_HISTOGRAM_METRIC_NAME)
                    .setDescription(TEST_EXPONENTIAL_HISTOGRAM_DESCRIPTION)
                    .setUnit("1")
                    .setExponentialHistogram(ExponentialHistogram.newBuilder()
                        .addDataPoints(ExponentialHistogramDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(TEST_EXPONENTIAL_HISTOGRAM_ATTR_KEY)
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue(TEST_EXPONENTIAL_HISTOGRAM_ATTR_VALUE)
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setTimeUnixNano(endUnixTimeNano)
                            .setCount(TEST_COUNT)
                            .setSum(TEST_SUM)
                            .setFlags(TEST_FLAGS)
                            .setMin(TEST_MIN)
                            .setMax(TEST_MAX)
                            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder()
                                    .addBucketCounts(POSITIVE_BUCKET_COUNT1)
                                    .addBucketCounts(POSITIVE_BUCKET_COUNT2)
                                    .addBucketCounts(POSITIVE_BUCKET_COUNT3).setOffset(POSITIVE_BUCKET_OFFSET).build())
                            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder()
                                    .addBucketCounts(NEGATIVE_BUCKET_COUNT1)
                                    .addBucketCounts(NEGATIVE_BUCKET_COUNT2)
                                    .addBucketCounts(NEGATIVE_BUCKET_COUNT3).setOffset(NEGATIVE_BUCKET_OFFSET).build())
                            .setScale(TEST_SCALE)
                            .build())
                            .setAggregationTemporality(io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName(TEST_HISTOGRAM_METRIC_NAME)
                    .setDescription(TEST_HISTOGRAM_DESCRIPTION)
                    .setUnit("1")
                    .setHistogram(Histogram.newBuilder()
                        .addDataPoints(HistogramDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(TEST_HISTOGRAM_ATTR_KEY)
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue(TEST_HISTOGRAM_ATTR_VALUE)
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setTimeUnixNano(endUnixTimeNano)
                            .setCount(TEST_COUNT)
                            .setSum(TEST_SUM)
                            .setFlags(TEST_FLAGS)
                            .setMin(TEST_MIN)
                            .setMax(TEST_MAX)
                            .addAllBucketCounts(TEST_BUCKET_COUNTS)
                            .addAllExplicitBounds(TEST_EXPLICIT_BOUNDS)
                            .build())
                            .setAggregationTemporality(io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName(TEST_SUMMARY_METRIC_NAME)
                    .setDescription(TEST_SUMMARY_DESCRIPTION)
                    .setUnit("1")
                    .setSummary(Summary.newBuilder()
                        .addDataPoints(SummaryDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey(TEST_SUMMARY_ATTR_KEY)
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue(TEST_SUMMARY_ATTR_VALUE)
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setTimeUnixNano(endUnixTimeNano)
                            .setCount(TEST_COUNT)
                            .setFlags(TEST_FLAGS)
                            .setSum(TEST_SUM)
                            .addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
                                    .setQuantile(TEST_QUANTILE)
                                    .setValue(TEST_QUANTILE_VALUE)
                                    .build()))
                        .build())
                    .build())
                .build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetrics)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
                .build();
    }
}
