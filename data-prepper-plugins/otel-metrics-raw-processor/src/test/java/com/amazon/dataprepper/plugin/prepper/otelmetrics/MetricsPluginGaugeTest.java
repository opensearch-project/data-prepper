/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin.prepper.otelmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;


@RunWith(MockitoJUnitRunner.class)
public class MetricsPluginGaugeTest {

    OTelMetricsStringProcessor stringPrepper;

    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        stringPrepper = new OTelMetricsStringProcessor(testsettings);
    }

    @Test
    public void test() throws JsonProcessingException {
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Sum sum = Sum.newBuilder().addDataPoints(p1).build();

        Metric.Builder metric = Metric.newBuilder()
                .setSum(sum)
                .setUnit("seconds")
                .setName("whatname")
                .setDescription("kronos");

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

        ExportMetricsServiceRequest exportMEtricRequest = ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics).build();

        Record<ExportMetricsServiceRequest> record = new Record<>(exportMEtricRequest);

        Collection<Record<String>> records = stringPrepper.doExecute(Arrays.asList(record));
        List<Record<String>> list = new ArrayList<>(records);

        Record<String> dataPrepperResult = list.get(0);

        ObjectMapper objectMapper = new ObjectMapper();
        Map map = objectMapper.readValue(dataPrepperResult.getData(), Map.class);

        assertSumProcessing(map);
    }

    private void assertSumProcessing(Map map) {
        assertThat(map).contains(entry("kind", "sum"));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("description", "kronos"));
        assertThat(map).contains(entry("name", "whatname"));
        assertThat(map).contains(entry("serviceName", "service"));
    }
}
