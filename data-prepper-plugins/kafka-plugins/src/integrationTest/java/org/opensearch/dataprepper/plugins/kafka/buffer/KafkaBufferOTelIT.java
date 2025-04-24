/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.model.buffer.DelegatingBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;


import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelTraceDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaBufferOTelIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBufferIT.class);
    private static long TIME_DELTA = 3600L;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private KafkaBufferConfig kafkaBufferConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private BufferTopicConfig topicConfig;

    private DelegatingBuffer buffer;

    private PluginMetrics pluginMetrics;
    private String bootstrapServersCommaDelimited;
    private Random random;
    private String logsRegion;
    private String logsServiceName;
    private int logsSeverityValue;
    private String logsSeverityText;
    private String logsBodyValue;
    private long currentUnixTimeNano;
    private Instant currentTime;
    private int logsDroppedAttributesCount;
    private String logsSchemaUrl;

    private String TraceId;
    private String SpanId;

    private String traceServiceName;
    private int traceDroppedAttributesCount;
    private String TraceId2;
    private String SpanId2;
    private String scopeSpanName;
    private String scopeName;
    private String scopeVersion;
    private String ilSpanName;
    private String ilName;
    private String ilVersion;

    private int metricsCountValue;
    private int metricsSumValue;
    private List<Long> histogramBucketCounts;
    private List<Double> histogramExplicitBounds;

    class KafkaDelegatingBuffer extends DelegatingBuffer {
        KafkaDelegatingBuffer(Buffer buffer) {
            super(buffer);
        }
    };

    @BeforeEach
    void setUp() {
        random = new Random();
        currentTime = Instant.now();
        currentUnixTimeNano = ((long)currentTime.getEpochSecond() * 1000_000_000L) + currentTime.getNano();

        TraceId = RandomStringUtils.randomNumeric(20);
        SpanId = RandomStringUtils.randomNumeric(10);
        TraceId2 = RandomStringUtils.randomNumeric(20);
        SpanId2 = RandomStringUtils.randomNumeric(10);
        scopeSpanName = RandomStringUtils.randomAlphabetic(10);
        scopeName = RandomStringUtils.randomAlphabetic(10);
        ilSpanName = RandomStringUtils.randomAlphabetic(10);
        ilName = RandomStringUtils.randomAlphabetic(10);
        scopeVersion = RandomStringUtils.randomNumeric(2);
        ilVersion = RandomStringUtils.randomNumeric(2);
        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        String topicName = "buffer-" + RandomStringUtils.randomAlphabetic(5);
        when(topicConfig.getName()).thenReturn(topicName);
        when(topicConfig.getGroupId()).thenReturn("buffergroup-" + RandomStringUtils.randomAlphabetic(6));
        when(topicConfig.isCreateTopic()).thenReturn(true);
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.BYTES);
        when(topicConfig.getWorkers()).thenReturn(1);
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(1);
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(kafkaBufferConfig.getTopic()).thenReturn(topicConfig);

        EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);

        bootstrapServersCommaDelimited = System.getProperty("tests.kafka.bootstrap_servers");

        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServersCommaDelimited);

        when(kafkaBufferConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServersCommaDelimited));
        when(kafkaBufferConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
    }

    private ExportMetricsServiceRequest createExportMetricsServiceRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("attribute1")
                        .setValue(AnyValue.newBuilder().setStringValue("attribute1-value").build())
                ).build();

        metricsCountValue = random.nextInt(1000);
        metricsSumValue = random.nextInt(10000);
        Long tmpSum = 0L;
        histogramExplicitBounds = List.of(10.0, 20.0, 30.0, 40.0);
        histogramBucketCounts = new ArrayList();
        for (int i = 0; i < histogramExplicitBounds.size(); i++) {
            Long value = (long)random.nextInt((int)(metricsSumValue/6));
            tmpSum += value;
            histogramBucketCounts.add(value);
        }
        histogramBucketCounts.add(metricsSumValue-tmpSum);
        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName("counter")
                    .setUnit("1")
                    .setGauge(Gauge.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey("gauge-1")
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue("gauge-value-1")
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setAsInt(metricsCountValue)
                            .build())
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName("sum")
                    .setUnit("1")
                    .setSum(Sum.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey("sum-1")
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue("sum-value-1")
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setAsInt(metricsSumValue)
                            .build())
                        .setAggregationTemporality(io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                        .build())
                    .build())
                .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                    .setName("histogram")
                    .setUnit("1")
                    .setHistogram(Histogram.newBuilder()
                        .addDataPoints(HistogramDataPoint.newBuilder()
                            .addAttributes(KeyValue.newBuilder()
                                .setKey("histogram-1")
                                .setValue(AnyValue.newBuilder()
                                    .setStringValue("histogram-value-1")
                                    .build()))
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setCount(metricsCountValue)
                            .setSum(metricsSumValue)
                            .addAllBucketCounts(histogramBucketCounts)
                            .addAllExplicitBounds(histogramExplicitBounds)
                            .build())
                            .setAggregationTemporality(io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
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

    private void validateMetric(Event event) {
        JacksonMetric metric = (JacksonMetric) (JacksonEvent)event;
        String metricKind = metric.getKind();
        assertTrue(metricKind.equals(Metric.KIND.GAUGE.toString()) ||
            metricKind.equals(Metric.KIND.SUM.toString()) ||
            metricKind.equals(Metric.KIND.HISTOGRAM.toString()));

        if (metric.getKind().equals(Metric.KIND.GAUGE.toString())) {
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("counter"));
            JacksonGauge gauge = (JacksonGauge)metric;
            assertThat(gauge.getValue(), equalTo((double)metricsCountValue));
        } else if (metric.getKind().equals(Metric.KIND.SUM.toString())) {
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("sum"));
            JacksonSum sum = (JacksonSum)metric;
            assertThat(sum.getValue(), equalTo((double)metricsSumValue));
        } else { // Histogram
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("histogram"));
            JacksonHistogram histogram = (JacksonHistogram)metric;
            assertThat(histogram.getSum(), equalTo((double)metricsSumValue));
            assertThat(histogram.getCount(), equalTo((long)metricsCountValue));
            assertThat(histogram.getExemplars(), equalTo(Collections.emptyList()));
            assertThat(histogram.getExplicitBoundsList(), equalTo(histogramExplicitBounds));
            assertThat(histogram.getExplicitBoundsCount(), equalTo(histogramExplicitBounds.size()));
            assertThat(histogram.getBucketCountsList(), equalTo(histogramBucketCounts));
            assertThat(histogram.getBucketCount(), equalTo(histogramBucketCounts.size()));
            assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
        }
    }

    @Test
    void test_otel_metrics_with_kafka_buffer() throws Exception {
        KafkaBuffer kafkaBuffer = new KafkaBuffer(pluginSetting, kafkaBufferConfig, acknowledgementSetManager, new OTelMetricDecoder(OTelOutputFormat.OPENSEARCH), null, null);
        buffer = new KafkaDelegatingBuffer(kafkaBuffer);
        final ExportMetricsServiceRequest request = createExportMetricsServiceRequest();
        buffer.writeBytes(request.toByteArray(), null, 10_000);
        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = kafkaBuffer.read(10_000);
        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(3));
        for (Record<Event> record : readResult.getKey()) {
            validateMetric(record.getData());
        }
    }

    private ExportLogsServiceRequest createExportLogsRequest() {
        logsServiceName = RandomStringUtils.randomAlphabetic(8);

        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue(logsServiceName).build())
                ).build();

        logsSeverityValue = random.nextInt(100000);
        logsSeverityText = RandomStringUtils.randomAlphabetic(8);
        logsSchemaUrl = RandomStringUtils.randomAlphabetic(9);
        logsBodyValue = RandomStringUtils.randomAlphabetic(10);
        logsDroppedAttributesCount = random.nextInt(1000);
        logsRegion = RandomStringUtils.randomNumeric(6);
        final ResourceLogs resourceLogs = ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .addLogRecords(LogRecord.newBuilder()
                            .setTimeUnixNano(currentUnixTimeNano)
                            .setObservedTimeUnixNano(currentUnixTimeNano+TIME_DELTA*1000_000_000)
                            .setSeverityNumberValue(logsSeverityValue)
                            .setSeverityText(logsSeverityText)
                            .setBody(AnyValue.newBuilder().setStringValue(logsBodyValue).build())
                            .setDroppedAttributesCount(logsDroppedAttributesCount)
                            .setTraceId(ByteString.copyFrom(TraceId.getBytes()))
                            .setSpanId(ByteString.copyFrom(SpanId.getBytes()))
                            .addAttributes(KeyValue.newBuilder()
                                .setKey("statement.region")
                                .setValue(AnyValue.newBuilder().setStringValue(logsRegion).build()).build())
                        .build()))
                .setResource(resource)
                .setSchemaUrl(logsSchemaUrl)
                .build();

        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
    }

    private void validateLog(OpenTelemetryLog logRecord) throws Exception {
        assertThat(logRecord.getServiceName(), is(logsServiceName));
        assertThat(logRecord.getTime(), is(currentTime.toString()));
        assertThat(logRecord.getObservedTime(), is((currentTime.plusSeconds(TIME_DELTA)).toString()));
        assertThat(logRecord.getBody(), is(logsBodyValue));
        assertThat(logRecord.getDroppedAttributesCount(), is(logsDroppedAttributesCount));
        assertThat(logRecord.getSchemaUrl(), is(logsSchemaUrl));
        assertThat(logRecord.getSeverityNumber(), is(logsSeverityValue));
        assertThat(logRecord.getSeverityText(), is(logsSeverityText));

        assertThat(new String(Hex.decodeHex(logRecord.getTraceId())), is(ByteString.copyFrom(TraceId.getBytes()).toStringUtf8()));
        assertThat(new String(Hex.decodeHex(logRecord.getSpanId())), is(ByteString.copyFrom(SpanId.getBytes()).toStringUtf8()));
        Map<String, Object> mergedAttributes = logRecord.getAttributes();
        assertThat(mergedAttributes.keySet().size(), is(2));
        assertThat(mergedAttributes.get("log.attributes.statement@region"), is(logsRegion));
        assertThat(mergedAttributes.get("resource.attributes.service@name"), is(logsServiceName));
    }

    @Test
    void test_otel_logs_with_kafka_buffer() throws Exception {
        KafkaBuffer kafkaBuffer = new KafkaBuffer(pluginSetting, kafkaBufferConfig, acknowledgementSetManager, new OTelLogsDecoder(OTelOutputFormat.OPENSEARCH), null, null);
        buffer = new KafkaDelegatingBuffer(kafkaBuffer);
        final ExportLogsServiceRequest request = createExportLogsRequest();
        buffer.writeBytes(request.toByteArray(), null, 10_000);
        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = kafkaBuffer.read(10_000);
        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(1));
        for (Record<Event> record : readResult.getKey()) {
            validateLog((OpenTelemetryLog)record.getData());
        }
    }

    private ExportTraceServiceRequest createExportTraceRequest() {
        traceServiceName = RandomStringUtils.randomAlphabetic(8);
        traceDroppedAttributesCount = random.nextInt(1000);

        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue(traceServiceName).build())
                )
                .setDroppedAttributesCount(traceDroppedAttributesCount)
                .build();

        final ScopeSpans scopeSpans = ScopeSpans.newBuilder()
                .setScope(InstrumentationScope.newBuilder()
                        .setName(scopeName)
                        .setVersion(scopeVersion)
                        .build())
                .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder()
                            .setTraceId(ByteString.copyFrom(TraceId.getBytes()))
                            .setSpanId(ByteString.copyFrom(SpanId.getBytes()))
                            .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_INTERNAL)
                            .setStartTimeUnixNano(currentUnixTimeNano)
                            .setName(scopeSpanName)
                            .setEndTimeUnixNano(currentUnixTimeNano+TIME_DELTA*1000_000_000)
                            .build())
                .build();

        ResourceSpans resourceSpans = ResourceSpans.newBuilder()
                .setResource(resource)
                .addScopeSpans(scopeSpans)
                .build();

        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(resourceSpans)
                .build();
    }

    private void validateSpan(Span span) throws Exception {
        assertThat(new String(Hex.decodeHex(span.get("traceId", String.class))), is(ByteString.copyFrom(TraceId.getBytes()).toStringUtf8()));
        assertThat(new String(Hex.decodeHex(span.get("spanId", String.class))), is(ByteString.copyFrom(SpanId.getBytes()).toStringUtf8()));
        assertThat(span.get("droppedLinksCount", Integer.class), is(0));
        assertThat(span.get("droppedAttributesCount", Integer.class), is(0));
        assertThat(span.get("kind", String.class), is("SPAN_KIND_INTERNAL"));
        assertThat(span.get("name", String.class), is(scopeSpanName));
        Map<String, Object> attributes = span.get("attributes", Map.class);

        assertThat(span.get("traceGroup", String.class), is(scopeSpanName));
        assertThat(attributes.get("instrumentationScope.name"), is(scopeName));
        assertThat(attributes.get("resource.attributes.service@name"), is(traceServiceName));
        assertThat(span.get("startTime", String.class), is(currentTime.toString()));
        assertThat(span.get("endTime", String.class), is((currentTime.plusSeconds(TIME_DELTA)).toString()));
        assertThat(span.get("durationInNanos", Long.class), is(TIME_DELTA*1000_000_000L));
        assertThat(span.get("parentSpanId", String.class), is(""));

    }

    @Test
    void test_otel_traces_with_kafka_buffer() throws Exception {
        KafkaBuffer kafkaBuffer = new KafkaBuffer(pluginSetting, kafkaBufferConfig, acknowledgementSetManager, new OTelTraceDecoder(OTelOutputFormat.OPENSEARCH), null, null);
        buffer = new KafkaDelegatingBuffer(kafkaBuffer);
        final ExportTraceServiceRequest request = createExportTraceRequest();
        buffer.writeBytes(request.toByteArray(), null, 10_000);
        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = kafkaBuffer.read(10_000);
        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(1));
        for (Record<Event> record : readResult.getKey()) {
            validateSpan((Span)record.getData());
        }
    }

}

