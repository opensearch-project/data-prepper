/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;


@RunWith(MockitoJUnitRunner.class)
public class MetricsPluginGaugeTest {

    private OTelMetricsRawProcessor rawProcessor;

    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        rawProcessor = new OTelMetricsRawProcessor(testsettings);
    }

    @Test
    public void testInstrumentationLibrary() throws JsonProcessingException {
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");

        InstrumentationLibraryMetrics isntLib = InstrumentationLibraryMetrics.newBuilder()
                .addMetrics(metric)
                .setInstrumentationLibrary(InstrumentationLibrary.newBuilder()
                        .setName("ilname")
                        .setVersion("ilversion")
                        .build())
                .build();

        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .addInstrumentationLibraryMetrics(isntLib)
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
        assertThat(map).contains(entry("startTime","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("instrumentationLibrary.name", "ilname"));
        assertThat(map).contains(entry("instrumentationLibrary.version", "ilversion"));

    }

    @Test
    public void testScopeMetricsLibrary() throws JsonProcessingException {
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
        assertThat(map).contains(entry("startTime","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("instrumentationScope.name", "smname"));
        assertThat(map).contains(entry("instrumentationScope.version", "smversion"));

    }
}
