/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin.prepper.otelmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazon.dataprepper.model.metric.Metric;
import io.opentelemetry.proto.metrics.v1.Gauge;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.otelmetrics.OTelMetricsRawProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;


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
    public void test() throws JsonProcessingException {
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");

        InstrumentationLibraryMetrics isntLib = InstrumentationLibraryMetrics.newBuilder().addMetrics(metric).build();

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
        Map map = objectMapper.readValue(dataPrepperResult.getData().toJsonString(), Map.class);
        assertSumProcessing(map);
    }

    private void assertSumProcessing(Map map) {
        assertThat(map).contains(entry("kind", Metric.KIND.GAUGE.toString()));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("serviceName", "service"));
        assertThat(map).contains(entry("resource.attributes.service@name", "service"));
        assertThat(map).contains(entry("description", "description"));
        assertThat(map).contains(entry("value", 4.0D));
        assertThat(map).contains(entry("startTime","1970-01-01T00:00:00Z"));
        assertThat(map).contains(entry("time","1970-01-01T00:00:00Z"));
    }
}
