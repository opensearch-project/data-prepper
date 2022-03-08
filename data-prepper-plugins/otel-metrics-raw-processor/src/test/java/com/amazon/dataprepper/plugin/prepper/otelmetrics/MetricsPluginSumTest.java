/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */
package com.amazon.dataprepper.plugin.prepper.otelmetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.otelmetrics.OTelMetricsStringProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

@RunWith(MockitoJUnitRunner.class)
public class MetricsPluginSumTest {

    OTelMetricsStringProcessor stringPrepper;

    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        stringPrepper = new OTelMetricsStringProcessor(testsettings);
    }

    @Test
    public void test() throws JsonProcessingException {
        NumberDataPoint dataPoint = NumberDataPoint.newBuilder().setAsInt(3).build();
        Gauge gauge = Gauge.newBuilder().addDataPoints(dataPoint).build();
        Metric metric = Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("whatname")
                .setDescription("kron")
                .build();

        InstrumentationLibraryMetrics instLib = InstrumentationLibraryMetrics.newBuilder()
                .addMetrics(metric).build();

        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addInstrumentationLibraryMetrics(instLib)
                .build();

        ExportMetricsServiceRequest exportMetricRequest = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();

        Record record = new Record<>(exportMetricRequest);

        List bla =  (List<Record<String>>) stringPrepper.doExecute(Arrays.asList(record));
        Record<String> recor = (Record<String>) bla.get(0);

        ObjectMapper objectMapper = new ObjectMapper();
        Map map = objectMapper.readValue(recor.getData(), Map.class);
        assertGaugeProcessing(map);
    }

    private void assertGaugeProcessing(Map map) {
        assertThat(map).contains(entry("kind", "gauge"));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("description", "kron"));
        assertThat(map).contains(entry("name", "whatname"));
        assertThat(map).contains(entry("serviceName", "service"));
    }

}
