/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

@ExtendWith(MockitoExtension.class)
class MetricsPluginGaugeTest {

    private static final Long START_TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 0, 0),
            ZoneOffset.UTC).toInstant().toEpochMilli());

    private static final Long TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 1, 0),
            ZoneOffset.UTC).toInstant().toEpochMilli());


    private OTelMetricsRawProcessor rawProcessor;
    private static final Random RANDOM = new Random();

    private byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    @BeforeEach
    void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        rawProcessor = new OTelMetricsRawProcessor(testsettings, new OtelMetricsRawProcessorConfig());
    }

    @Test
    void testScopeMetricsLibrary() throws JsonProcessingException {
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");

        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .setScope(InstrumentationScope.newBuilder()
                        .setName("smname")
                        .setVersion("smversion"))
                .build();

        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .addScopeMetrics(scopeMetrics)
                .setResource(resource)
                .build();

        ExportMetricsServiceRequest exportMetricRequest = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics).build();

        Record<ExportMetricsServiceRequest> record = new Record<>(exportMetricRequest);

        Collection<Record<? extends Metric>> records = rawProcessor.doExecute(Collections.singletonList(record));
        List<Record<? extends Metric>> list = new ArrayList<>(records);

        Record<? extends Metric> dataPrepperResult = list.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> map = objectMapper.readValue(dataPrepperResult.getData().toJsonString(), Map.class);

        assertThat(map).contains(entry("kind", Metric.KIND.GAUGE.toString()));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("serviceName", "service"));
        assertThat(map).contains(entry("resource.attributes.service@name", "service"));
        assertThat(map).contains(entry("description", "description"));
        assertThat(map).contains(entry("value", 4.0D));
        assertThat(map).contains(entry("startTime", "1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time", "1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("instrumentationScope.name", "smname"));
        assertThat(map).contains(entry("instrumentationScope.version", "smversion"));

    }

    @Test
    void testWithExemplar() throws JsonProcessingException {

        byte[] spanId = getRandomBytes(8);
        byte[] traceId = getRandomBytes(8);

        Exemplar e1 = Exemplar.newBuilder()
                .addFilteredAttributes(KeyValue.newBuilder()
                        .setKey("key")
                        .setValue(AnyValue.newBuilder().setBoolValue(true).build()).build())
                .setAsDouble(3)
                .setSpanId(ByteString.copyFrom(spanId))
                .setTimeUnixNano(TIME)
                .setTraceId(ByteString.copyFrom(traceId))
                .build();

        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder()
                .addExemplars(e1)
                .setStartTimeUnixNano(START_TIME)
                .setTimeUnixNano(TIME)
                .setAsInt(4)
                .setFlags(1);

        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");

        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .setScope(InstrumentationScope.newBuilder()
                        .setName("smname")
                        .setVersion("smversion"))
                .build();

        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .addScopeMetrics(scopeMetrics)
                .setResource(resource)
                .build();

        ExportMetricsServiceRequest exportMetricRequest = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics).build();

        Record<ExportMetricsServiceRequest> record = new Record<>(exportMetricRequest);

        Collection<Record<? extends Metric>> records = rawProcessor.doExecute(Collections.singletonList(record));
        List<Record<? extends Metric>> list = new ArrayList<>(records);

        Record<? extends Metric> dataPrepperResult = list.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> map = objectMapper.readValue(dataPrepperResult.getData().toJsonString(), Map.class);

        assertThat(map).contains(entry("kind", Metric.KIND.GAUGE.toString()));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("serviceName", "service"));
        assertThat(map).contains(entry("resource.attributes.service@name", "service"));
        assertThat(map).contains(entry("description", "description"));
        assertThat(map).contains(entry("value", 4.0D));
        assertThat(map).contains(entry("startTime", "2020-05-24T14:00:00Z"));
        assertThat(map).contains(entry("time", "2020-05-24T14:01:00Z"));
        assertThat(map).contains(entry("instrumentationScope.name", "smname"));
        assertThat(map).contains(entry("instrumentationScope.version", "smversion"));
        assertThat(map).contains(entry("flags", 1));

        List<Map<String, Object>> exemplars = (List<Map<String, Object>>) map.get("exemplars");
        assertThat(exemplars.size()).isEqualTo(1);
        Map<String, Object> eTest = exemplars.get(0);

        assertThat(eTest).contains(entry("time", "2020-05-24T14:01:00Z"));
        assertThat(eTest).contains(entry("value", 3.0));
        assertThat(eTest).contains(entry("spanId", Hex.encodeHexString(spanId)));
        assertThat(eTest).contains(entry("traceId", Hex.encodeHexString(traceId)));
        Map<String, Object> atts = (Map<String, Object>) eTest.get("attributes");
        assertThat(atts).contains(entry("exemplar.attributes.key", true));
    }
}
