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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

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
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawHistogram;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

@RunWith(MockitoJUnitRunner.class)
public class MetricsPluginHistogramTest {

    OTelMetricsStringProcessor stringPrepper;

    @Before
    public void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        stringPrepper = new OTelMetricsStringProcessor(testsettings);
    }

    @Test
    public void test() throws JsonProcessingException {

        final double bound_0 = 10.0;
        final double bound_1 = 100.0;
        final double bound_2 = 1000.0;
        HistogramDataPoint dp = HistogramDataPoint.newBuilder()
                .addBucketCounts(3)
                .addBucketCounts(5)
                .addBucketCounts(17)
                .addBucketCounts(33)
                .addExplicitBounds(bound_0)
                .addExplicitBounds(bound_1)
                .addExplicitBounds(bound_2)
                .setCount(4)
                .build();

        Histogram histogram = Histogram.newBuilder().addDataPoints(dp).build();

        Record<ExportMetricsServiceRequest> record = new Record<>(fillServiceRequest(histogram));

        List<Record<String>> processedRecords =  (List<Record<String>>) stringPrepper.doExecute(Collections.singletonList(record));
        Record<String> recor = processedRecords.get(0);

        ObjectMapper objectMapper = new ObjectMapper();
        Map<Object, Object> map = objectMapper.readValue(recor.getData(), Map.class);

        RawHistogram.Bucket bucket_0 = new RawHistogram.Bucket(0.0, 0.0, 3);
        RawHistogram.Bucket bucket_1 = new RawHistogram.Bucket(0.0, bound_0, 5);
        RawHistogram.Bucket bucket_2 = new RawHistogram.Bucket(bound_0, bound_1, 17);
        RawHistogram.Bucket bucket_3 = new RawHistogram.Bucket(bound_1, bound_2, 33);
        System.out.println(map);
        assertHistogramProcessing(map, Arrays.asList(bucket_0, bucket_1, bucket_2, bucket_3));
    }

    private ExportMetricsServiceRequest fillServiceRequest(Histogram histogram) {
        Metric metric = Metric.newBuilder()
                .setHistogram(histogram)
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
        return ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    }

    private void assertHistogramProcessing(Map<Object, Object> map, List<RawHistogram.Bucket> expectedBuckets) {
        assertThat(map).contains(entry("kind", "histogram"));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("description", "kron"));
        assertThat(map).contains(entry("name", "whatname"));
        assertThat(map).contains(entry("serviceName", "service"));

        assertThat(map).containsKey("values");

        List<Map> listOfMaps = (List<Map>) map.get("values");
        assertThat(listOfMaps).hasSize(expectedBuckets.size());

        for (int i = 0; i < expectedBuckets.size(); i++) {
            RawHistogram.Bucket expectedBucket = expectedBuckets.get(i);
            Map<Object, Object> actualBucket = listOfMaps.get(i);

            assertThat(actualBucket)
                    .contains(entry("lo", expectedBucket.getLowerBound()))
                    .contains(entry("hi", expectedBucket.getUpperBound()))
                    .contains(entry("cnt", (int)expectedBucket.getNumberOfObservations()));

        }
    }
}
